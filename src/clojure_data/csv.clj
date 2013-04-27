(ns clojure-data.csv
  (require [clojure-csv.core :as csv]
           [clojure.string :as string]))

(def parse-csv
  csv/parse-csv)

(defn parse-tsv [tsv]
  (map #(string/split % #"\t") (string/split-lines tsv)))
