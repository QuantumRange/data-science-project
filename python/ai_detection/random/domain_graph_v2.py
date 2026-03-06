# @author ChatGPT
# Only for testing, no code for real analytics!

import sys
import glob
import time
import math
import colorsys

import tldextract
import pyarrow.parquet as pq
import polars as pl
import igraph as ig
import networkx as nx

PARQUET_FILES = sorted(glob.glob("/Users/qr/Downloads/chunks/*.parquet"))

_extractor = tldextract.TLDExtract(suffix_list_urls=None)


def _reg_domain(host: str) -> str:
    ext = _extractor(host)
    return ext.top_domain_under_public_suffix or host


def build_graph(files: list[str]) -> ig.Graph:
    t0 = time.perf_counter()

    # Pass 1: collect unique hosts
    host_mapping = (
        pl.scan_parquet(files)
        .select(
            pl.col("domain"),
            pl.col("url").str.extract(r"://([^/:]+)", 1).alias("host"),
        )
        .drop_nulls("host")
        .unique("host")
        .collect(engine="streaming")
    )

    # Map each host to its registered domain (combines subdomains)
    hosts = host_mapping["host"].to_list()
    reg_domains = [_reg_domain(h) for h in hosts]
    host_mapping = host_mapping.with_columns(pl.Series("reg_domain", reg_domains))

    t1 = time.perf_counter()
    n_reg = len(set(reg_domains))
    print(f"  Mapped {len(host_mapping):,} hosts -> {n_reg:,} domains ({t1 - t0:.1f}s)")

    # Join tables for Pass 2
    src_map = (
        host_mapping.select(pl.col("domain"), pl.col("reg_domain").alias("src_domain"))
        .unique("domain")
    )
    tgt_map = (
        host_mapping.select(pl.col("host").alias("tgt_host"), pl.col("reg_domain").alias("tgt_domain"))
        .unique("tgt_host")
    )

    # Pass 2: extract edges using registered domains
    edge_weights = (
        pl.scan_parquet(files)
        .select(pl.col("domain"), "links")
        .drop_nulls("links")
        .join(src_map.lazy(), on="domain", how="inner")
        .drop("domain")
        .with_columns(pl.col("links").str.extract_all(r'://[^/:\"]+').alias("tgt_hosts"))
        .drop("links")
        .explode("tgt_hosts")
        .drop_nulls("tgt_hosts")
        .with_columns(pl.col("tgt_hosts").str.strip_prefix("://"))
        .rename({"tgt_hosts": "tgt_host"})
        .join(tgt_map.lazy(), on="tgt_host", how="inner")
        .drop("tgt_host")
        .filter(pl.col("src_domain") != pl.col("tgt_domain"))
        .group_by("src_domain", "tgt_domain")
        .agg(pl.len().alias("weight"))
        .collect(engine="streaming")
    )

    t2 = time.perf_counter()
    print(f"  {len(edge_weights):,} edges ({t2 - t1:.1f}s)")

    if edge_weights.is_empty():
        return ig.Graph(directed=True)

    all_domains = sorted(
        set(edge_weights["src_domain"].to_list()) | set(edge_weights["tgt_domain"].to_list())
    )
    domain_idx = {d: i for i, d in enumerate(all_domains)}

    g = ig.Graph(n=len(all_domains), directed=True)
    g.vs["label"] = all_domains

    src = edge_weights["src_domain"].to_list()
    tgt = edge_weights["tgt_domain"].to_list()
    g.add_edges([(domain_idx[s], domain_idx[t]) for s, t in zip(src, tgt)])
    g.es["weight"] = edge_weights["weight"].to_list()

    return g


def simulate(g: ig.Graph):
    pr = g.pagerank(weights="weight" if g.ecount() > 0 else None)
    g.vs["pagerank"] = pr


def _cluster_color(idx: int) -> tuple[int, int, int]:
    hue = (idx * 0.618033988749895) % 1.0
    r, g, b = colorsys.hls_to_rgb(hue, 0.5, 0.7)
    return int(r * 255), int(g * 255), int(b * 255)

def export_gexf(
        g: ig.Graph,
        output: str = "domain_graph_top5000.gexf",
        top_k: int | None = 5000,
        cluster_on_full_graph: bool = False,
):
    # ---- Optional: compute clusters on full graph first (more expensive, but consistent) ----
    if cluster_on_full_graph:
        g_undir_full = g.as_undirected(combine_edges="sum")
        communities_full = g_undir_full.community_leiden(
            weights="weight", objective_function="modularity"
        )
        g.vs["cluster"] = communities_full.membership

    # ---- Filter to top_k nodes by existing PageRank ----
    if top_k is not None and g.vcount() > top_k:
        if "pagerank" not in g.vs.attributes():
            raise RuntimeError("PageRank not found. Run simulate(g) before export_gexf().")

        order = sorted(
            range(g.vcount()),
            key=lambda i: g.vs[i]["pagerank"],
            reverse=True,
        )
        keep = order[:top_k]
        g = g.induced_subgraph(keep)

    # ---- Community detection (if not done on full graph) ----
    if "cluster" not in g.vs.attributes():
        g_undir = g.as_undirected(combine_edges="sum")
        communities = g_undir.community_leiden(weights="weight", objective_function="modularity")
        g.vs["cluster"] = communities.membership

    n_clusters = (max(g.vs["cluster"]) + 1) if g.vcount() > 0 else 0
    print(f"  Exporting {g.vcount():,} nodes, {g.ecount():,} edges")
    print(f"  {n_clusters} clusters detected")

    # ---- Node size from PageRank (log-scaled) ----
    pr = g.vs["pagerank"] if "pagerank" in g.vs.attributes() else [0.0] * g.vcount()
    pr_max = max(pr) if pr else 0.0
    if pr_max > 0:
        g.vs["size"] = [
            10 + 90 * math.log1p(p / pr_max * 1000) / math.log1p(1000)
            for p in pr
        ]
    else:
        g.vs["size"] = [10.0] * g.vcount()

    # ---- RGB color per node from cluster ----
    for v in g.vs:
        r, gr, b = _cluster_color(v["cluster"])
        v["r"], v["g"], v["b"] = r, gr, b

    # ---- Edge visual weight (log-scale) ----
    if g.ecount() > 0:
        w_max = max(g.es["weight"])
        if w_max > 1:
            log_max = math.log1p(w_max)
            g.es["edge_weight_visual"] = [
                1.0 + 5.0 * math.log1p(w) / log_max for w in g.es["weight"]
            ]

    # ---- Convert to NetworkX for GEXF export ----
    nxg = nx.DiGraph()
    for v in g.vs:
        nxg.add_node(
            v["label"],
            label=v["label"],
            size=v["size"],
            pagerank=v["pagerank"],
            cluster=v["cluster"],
            viz={
                "color": {"r": v["r"], "g": v["g"], "b": v["b"], "a": 1.0},
                "size": v["size"],
            },
        )

    for e in g.es:
        src = g.vs[e.source]["label"]
        tgt = g.vs[e.target]["label"]
        attrs = {"weight": e["weight"]}
        if "edge_weight_visual" in e.attributes():
            attrs["edge_weight_visual"] = e["edge_weight_visual"]
        nxg.add_edge(src, tgt, **attrs)

    nx.write_gexf(nxg, output)
    print(f"Saved: {output}")

def main():
    files = sys.argv[1:] if len(sys.argv) > 1 else PARQUET_FILES
    if not files:
        print("No parquet files found. Pass paths as arguments or update PARQUET_FILES.")
        sys.exit(1)

    valid = []
    for f in files:
        try:
            pq.read_metadata(f)
            valid.append(f)
        except Exception:
            print(f"  Skipping broken file: {f}")
    files = valid

    print(f"Processing {len(files)} file(s)...")
    g = build_graph(files)
    print(f"Graph: {g.vcount()} nodes, {g.ecount()} edges")

    print("Running PageRank...")
    simulate(g)
    print("Exporting for Gephi...")
    export_gexf(g)


if __name__ == "__main__":
    main()

