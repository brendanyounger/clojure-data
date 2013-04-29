(ns clojure-data.peg-test
  (require [clojure.test :refer :all]
           [clojure-data.peg :refer :all]
           [clojure-data.csv :refer :all]
           [taoensso.timbre.profiling :refer [profile]]))

(def balanced-parens
  (grammar :S
    { :S (concat (choice (call :B) (regexp #"[^()]")))
      :B (concat (literal "(") (call :S) (literal ")")) }))

(def tsv
  (grammar :S
    { :S      (rule (rep* (call :line)))
      :line   (rule (rep* (call :field)) (literal "\n"))
      :field  (rule (choice (literal "\t") (concat (regexp #"[^\t\n]+") (call :field)))) }))

; CSV record with captures
; record <- (<field> (’,’ <field>)*)->{} (%nl / !.)
; field <- <escaped> / <nonescaped>
; nonescaped <- { [^,"%nl]* }
; escaped <- ’"’ {~ ([^"] / ’""’->’"’)* ~} ’"’

(def csv
  (grammar :csv
    { :csv        (rep* :record)
      :record     [:field (rep* \, :field) (choice "\n" (peek-not (any 1)))]
      :field      (choice :escaped :nonescaped)
      :nonescaped [(re-pattern "[^,\"\\n]*")]
      :escaped    [\" (re-pattern "([^\"]|\"\")*") \"]
    }))

;; current record: 52 ms
;; adding captures per rule: 60% slower
;; for parse-tsv: 6 ms
(deftest timing
  (testing "Testing PEG speed"
    (println tsv)
    (let [data    (slurp "test/blogdata.txt")
          start   (System/currentTimeMillis)
          result  (parse tsv data)
          stop    (System/currentTimeMillis)]
      (println (format "tsv: %d ms" (- stop start)))
      (is (= "success" result)))))
