(require hyrule [->])

(import polars :as pl)
(import pathlib [Path])
(import tqdm [tqdm])
(import tldextract)

(defn pipeline [df]
  (setv lang-type (. (. df schema) (get "lang")))

  (setv df (if (= lang-type pl.List)
             (.with-columns df :lang (.first (. (pl.col "lang") list)))
             df))
        
  (-> df
    (.filter (= (.field (. (pl.col "lang") struct) "lang") "en"))
    (.with-columns :root-host (.map-elements (pl.col "host") (fn [url]
                                                               (setv result (tldextract.extract url))
                                                               (+ result.domain "." result.suffix)) :return-dtype pl.String))
    (.with-columns :lang-en (.field (. (pl.col "lang") struct) "score"))
    (.with-columns :scores (.rename-fields (. (pl.col "label_scores") struct) ["human" "ai"]))
    (.unnest "scores")
    (.select "root_host" "host" "url" "text" "lang_en" "ai" "outflow")))

   

(defn main []
  (setv files (sorted (list (.glob (Path "/mnt/Fast2T/data/stage_3/") "*.parquet"))))
  (setv files (list (filter (fn [file] (not (.exists (/ (Path "/mnt/Fast2T/data/stage_4/") (. file name))))) files)))
  (for [file (tqdm files)]
   (setv df (pipeline (pl.read_parquet file)))
   (.write_parquet df (/ (Path "/mnt/Fast2T/data/stage_4/") (. file name)))))
   

(when (= __name__ "__main__")
  (main))
