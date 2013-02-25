(ns clojure-data.sql
  (require [clojure.string :as string]))

;; TODO: implement alter table, insert, update, delete

(defrecord SQLFragment [^String sql parameters])

(defn- to-fragment [value]
  (cond
    (nil? value)      (SQLFragment. "" [])
    (keyword? value)  (SQLFragment. (name value) [])
    (float? value)    (SQLFragment. (str value) [])
    (integer? value)  (SQLFragment. (str value) [])
    (string? value)   (SQLFragment. (str "'" (string/replace value "'" "''") "'") [])
    (instance? SQLFragment value) value))

(defn- safe-format [fmt-str & values]
  (let [fragments (map to-fragment values)]
    (SQLFragment.
      (apply format fmt-str (map :sql fragments))
      (apply concat (map :parameters fragments)))))

(defn- safe-infix [infix values]
  (let [fragments (map to-fragment values)]
    (SQLFragment.
      (string/join infix (map :sql fragments))
      (apply concat (map :parameters fragments)))))

(defn bin-op [op lhs rhs]
  (safe-format "%s %s %s" lhs op rhs))

(defn select [fields & clauses]
  (safe-format "select %s" (safe-infix "\n" (cons (safe-infix ", "fields) clauses))))

;; distinct

(defn ? [parameter]
  (SQLFragment. "?" [parameter]))

(defn from [table-name]
  (safe-format "from %s" table-name))

(defn join [table-name]
  (safe-format "join %s" table-name))

(defn left-join [table-name]
  (safe-format "left join %s" table-name))

(defn where [clause]
  (if clause
      (safe-format "where %s" clause)
      (SQLFragment. "" [])))

(defn not [clause] (safe-format "not %s" clause))
(defn exists [clause] (safe-format "exists (%s)" clause))

(defn order-by [& fields]
  (safe-format "order by %s" (safe-infix ", " fields)))

(defn having [clause]
  (safe-format "having %s" clause))

(defn limit [n]
  { :pre (integer? n) }
  (safe-format "limit %s" n))

(defn offset [n]
  { :pre (integer? n) }
  (safe-format "offset %s" n))

;; infix operations
(defn || [& args] (safe-infix " || " args))
(defn or [& clauses] (safe-infix " or " clauses))
(defn and [& clauses] (safe-infix " and " clauses))

;; binary operations
(defn as [lhs rhs] (bin-op :as lhs rhs))
(defn = [lhs rhs]
  (if (nil? rhs)
      (safe-format "%s is null" lhs)
      (bin-op := lhs rhs)))
(defn < [lhs rhs] (bin-op :< lhs rhs))
(defn <= [lhs rhs] (bin-op :<= lhs rhs))
(defn > [lhs rhs] (bin-op :> lhs rhs))
(defn >= [lhs rhs] (bin-op :>= lhs rhs))
(defn like [lhs rhs] (bin-op :like lhs rhs))
(defn ilike [lhs rhs] (bin-op :ilike lhs rhs))
(defn cast [arg type]
  (safe-format "%s::%s" arg type))

;; general functions
(defn call [fn-name & args]
  (safe-format "%s(%s)" fn-name (safe-infix ", " args)))
