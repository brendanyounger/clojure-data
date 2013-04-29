(ns clojure-data.relation)

(defprotocol Relation
  (union [lhs rhs] "set union of two relations")
  (intersection [lhs rhs] "")
  (difference [lhs rhs] "")
  (select [this horn-clauses] "")
  (project [this fields])
  (join [relations] "cartesian product, select later based on shared attributes?")
  ;; others: split (one set with, one set without), factor (new generated column)
  )

(extend-protocol Relation)
