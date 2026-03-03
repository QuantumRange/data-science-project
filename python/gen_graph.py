import sys
import pandas as pd
import networkit as nk
import json
import re
import numpy as np
from collections import Counter

if len(sys.argv) >= 1:
    datafile = sys.argv[1]
else:
    sys.exit("Missing one argument. The first argument should be the data file.")

# Read data and keep just the url features.
df = pd.read_parquet(datafile, engine="fastparquet")
print(len(df))

df = df[(df.status >= 200) & (df.status < 300)]
df.drop([
    "id", "domain", "timestamp", "duration", "http_version", "status", "header", "type", "text"
], axis=1, inplace=True)

# Reduce urls to juts their domain and subdomain part.
domain = re.compile(r'.*//([^/\?]*)(/|\?)?.*')
get_domain = lambda x: domain.match(x).groups()[0]

df["url"] = df["url"].apply(get_domain)
df["links"] = df["links"].apply(
    lambda x: set(map(get_domain, json.loads(x)))
)

# Create unique sorted url index for fast lookups.
df.sort_values("url", inplace=True)
df.reset_index(inplace=True)
df.drop('index', axis=1, inplace=True)
index = np.unique(df.url.to_numpy())

G = nk.Graph(len(index), directed=True)

# Aggregate data points with the same url and create a corresponding weighted
# edge in the graph.
for i, url in enumerate(index):
    il = df.url.searchsorted(url, side="left")
    ir = df.url.searchsorted(url, side="right")
    
    c = Counter()
    for idx in range(il, ir):
        for l in df.links.iloc[idx]:
            c[l] += 1
    c = dict(c)

    # Calculate edge weights
    for k,v in c.items():
        w = v / (ir - il)
        j = index.searchsorted(k)
        if j >= 0 and j < len(index) and index[j] == k:
            G.addEdge(i, j, w)

SCC = nk.components.StronglyConnectedComponents(G)
SCC.run()
cs = [c for c in SCC.getComponents() if len(c) > 1]
for c in cs:
    print([index[i] for i in c])
