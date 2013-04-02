(ns clojure-data.schema
  (import [java.sql SQLException])
  (require [clojure-data.sql :refer :all]
           [clojure-data.core :refer [execute]]))

;; password hashing should use pgcrypto

(defrecord RelationSchema [name log-name key-fields value-fields])

(defn relation-schema [name key-fields value-fields]
  (RelationSchema. name (keyword (str (#'clojure.core/name name) "_log")) key-fields value-fields))

(def bib (relation-schema :bibliography { :title :string } { :url :string :description :string }))

(defn- fields [^RelationSchema relation]
  (merge (:key-fields relation) (:value-fields relation)))

(defn- key-fields [^RelationSchema relation]
  (:key-fields relation))

(defn- value-fields [^RelationSchema relation]
  (:value-fields relation))

(defn- log-fields [^RelationSchema relation]
  (merge (fields relation) { :tx :integer :retracted :boolean }))

(defn- log-key-fields [^RelationSchema relation]
  (assoc (:key-fields relation) :tx :integer))

(defn- log-value-fields [^RelationSchema relation]
  (assoc (:value-fields relation) :retracted :boolean))

(def type->sql { :string :varchar :integer :int :boolean :bool })

(defn setup! []
  (execute (raw "create sequence timestep start 1")))

(defn tick! []
  (:timestep (first (execute (raw "select nextval('timestep') as timestep")))))

(defn create! [^RelationSchema relation]
  (let [fields-str
        (map
          (fn [[k v]] (safe-format "%s %s" k (type->sql v)))
          (fields relation))
        log-fields-str
        (map
          (fn [[k v]] (safe-format "%s %s" k (type->sql v)))
          (log-fields relation))
        primary-keys (safe-infix ", " (keys (key-fields relation)))
        log-primary-keys (safe-infix ", " (keys (log-key-fields relation)))]
    (execute
      (apply create-table (:name relation)
        (concat
          fields-str
          [(safe-format "primary key (%s)" primary-keys)])))
    (execute
      (apply create-table (:log-name relation)
        (concat
          log-fields-str
          [(safe-format "primary key (%s)" log-primary-keys)])))))

;; TODO: need to force the tuple to match the schema...
(defn upsert! [relation tuple]
  (let [timestep  (tick!)
        log-tuple (merge tuple { :tx timestep :retracted false })]
  (execute
    (safe-format
      "insert into %s (%s) values (%s)"
      (:log-name relation)
      (safe-infix ", " (keys log-tuple))
      (safe-infix ", " (vals log-tuple))))
  (try
    (execute
      (safe-format
        "update %s set %s where %s"
        (:name relation)
        (safe-infix ",\n"
          (map (fn [[k v]] (equal? k v)) (select-keys tuple (keys (value-fields relation)))))
        (safe-infix ", "
          (map (fn [[k v]] (equal? k v)) (select-keys tuple (keys (key-fields relation)))))
        ))
    (catch SQLException e
      (execute
        (safe-format
          "insert into %s (%s) values (%s)"
          (:name relation)
          (safe-infix ", " (keys tuple))
          (safe-infix ", " (vals tuple))))))))

(defn delete! [relation tuple]
  (let [timestep (tick!)
        log-tuple (merge tuple { :tx timestep :retracted true })]
  (execute
    (safe-format
      "insert into %s (%s) values (%s)"
      (:log-name relation)
      (safe-infix ", " (keys log-tuple))
      (safe-infix ", " (vals log-tuple))))
  (execute
    (safe-format
      "delete from %s where %s"
      (:name relation)
      (safe-infix ", "
        (map (fn [[k v]] (equal? k v)) (select-keys tuple (keys (key-fields relation)))))))))

(defn quondam [relation timestep]
  (remove :retracted
    (execute
      (safe-format
        "select %s, first(retracted order by tx desc) as retracted from %s where %s group by %s"
        (safe-infix ", "
          (map #(safe-format "first(%s order by tx desc) as %s" % %) (keys (fields relation))))
        (:log-name relation)
        (lte? :tx timestep)
        (safe-infix ", " (keys (key-fields relation)))))))

;; basics: simple types int, varchar, numeric, float, json, etc.
;; add a mapping from fields to clojure data structures
;; be able to serialize as json (including id attribute for ember)
