(ns clojure-data.sql
  (require [clojure.string :as string]))

(defrecord SQLFragment [^String sql parameters])

(defn- quote-identifier [^String identifier]
  (if (re-seq #"\s|\?" identifier)
      (format "\"%s\"" identifier)
      identifier))

(defn- to-fragment [value]
  (cond
    (= java.lang.Boolean (type value))
                      (SQLFragment. (str value) [])
    (nil? value)      (SQLFragment. "null" [])
    (keyword? value)  (SQLFragment. (if (namespace value)
                                        (str  (quote-identifier (namespace value))
                                              "."
                                              (quote-identifier (name value)))
                                        (quote-identifier (name value)))
                                    [])
    (float? value)    (SQLFragment. (str value) [])
    (integer? value)  (SQLFragment. (str value) [])
    (string? value)   (SQLFragment. (str "'" (string/replace value "'" "''") "'") [])
    (instance? SQLFragment value) value))

(defn- keys-to-vals [m ks] (map #(m %) ks))

(defn safe-format [fmt-str & values]
  (let [fragments (map to-fragment values)]
    (SQLFragment.
      (apply format fmt-str (map :sql fragments))
      (apply concat (map :parameters fragments)))))

(defn safe-infix [infix values]
  (let [fragments (map to-fragment values)]
    (SQLFragment.
      (string/join infix (map :sql fragments))
      (apply concat (map :parameters fragments)))))

(defn raw [sql & [params]]
  (SQLFragment. sql params))

(defn bin-op [op lhs rhs]
  (safe-format "%s %s %s" lhs op rhs))

(defn select [fields & clauses]
  (safe-format "select %s" (safe-infix "\n" (cons (safe-infix ", " fields) clauses))))

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

(defn not? [clause] (safe-format "not %s" clause))
(defn exists? [clause] (safe-format "exists (%s)" clause))

(defn order-by [& fields]
  (safe-format "order by %s" (safe-infix ", " fields)))

(defn asc [expr]
  (safe-format "%s asc" expr))

(defn desc [expr]
  (safe-format "%s desc" expr))

(defn having [clause]
  (safe-format "having %s" clause))

(defn limit [^Integer n]
  (safe-format "limit %s" n))

(defn offset [^Integer n]
  (safe-format "offset %s" n))

;; infix operations
(defn || [& args] (safe-infix " || " (remove nil? args)))
(defn or? [& clauses] (safe-infix " or " (remove nil? clauses)))
(defn and? [& clauses] (safe-infix " and " (remove nil? clauses)))

;; binary operations
(defn as [lhs rhs] (bin-op :as lhs rhs))
(defn coerce [arg type] (safe-format "cast %s as %s" arg type))

;; predicates
(defn equal? [lhs rhs]
  (if (nil? rhs)
      (safe-format "%s is null" lhs)
      (bin-op := lhs rhs)))
(defn lt? [lhs rhs] (bin-op :< lhs rhs))
(defn lte? [lhs rhs] (bin-op :<= lhs rhs))
(defn gt? [lhs rhs] (bin-op :> lhs rhs))
(defn gte? [lhs rhs] (bin-op :>= lhs rhs))
(defn like? [lhs rhs] (bin-op :like lhs rhs))
(defn ilike? [lhs rhs] (bin-op :ilike lhs rhs))

;; general functions
(defn call [fn-name & args]
  (safe-format "%s(%s)" fn-name (safe-infix ", " args)))

;; table alteration
(defn create-table [table & fields]
  (safe-format "create table %s (%s)" table (safe-infix ",\n" fields)))

(defn insert [table rows]
  (let [fields  (keys (first rows))]
    (safe-format "insert into %s (%s) values %s" table (safe-infix ", " fields)
      (safe-infix ",\n"
        (map
          #(safe-format "(%s)"
            (safe-infix ", "
              (keys-to-vals % fields)))
          rows)))))

(defn update [table field-to-value & clauses]
  (safe-format "update %s set %s\n%s"
    table
    (safe-infix ",\n" (map (fn [[k v]] (safe-format "%s = %s" k v)) field-to-value))
    (safe-infix "\n" clauses)))

(defn delete [table & clauses]
  (safe-format "delete from %s\n%s"
    table
    (safe-infix "\n" clauses)))

(defn upsert [table primary-key rows]
  (let [fields (keys (first rows))]
    (safe-infix ";\n"
      (map
        #(safe-format "%s;\n%s"
          (update table
                  (dissoc % primary-key)
                  (where (equal? primary-key (get % primary-key))))
          (safe-format "insert into %s (%s)\n%s"
            table
            (safe-infix ", " fields)
            (select (keys-to-vals % fields)
              (where (not? (exists?
                (select [1] (from table) (where (equal? primary-key (get % primary-key))))))))))
        rows))))
