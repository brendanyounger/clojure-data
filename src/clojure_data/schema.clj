(ns clojure-data.schema
  (import [java.sql SQLException])
  (require [clojure-data.sql :as sql]
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
  (execute (sql/raw "create sequence timestep start 1")))

(defn tick! []
  (:timestep (first (execute (sql/raw "select nextval('timestep') as timestep")))))

(defn create! [^RelationSchema relation]
  (let [fields-str
        (map
          (fn [[k v]] (sql/safe-format "%s %s" k (type->sql v)))
          (fields relation))
        log-fields-str
        (map
          (fn [[k v]] (sql/safe-format "%s %s" k (type->sql v)))
          (log-fields relation))
        primary-keys (sql/safe-infix ", " (keys (key-fields relation)))
        log-primary-keys (sql/safe-infix ", " (keys (log-key-fields relation)))]
    (execute
      (apply sql/create-table (:name relation)
        (concat
          fields-str
          [(sql/safe-format "primary key (%s)" primary-keys)])))
    (execute
      (apply sql/create-table (:log-name relation)
        (concat
          log-fields-str
          [(sql/safe-format "primary key (%s)" log-primary-keys)])))))

;; TODO: need to force the tuple to match the schema...
(defn upsert! [relation tuple]
  (let [timestep  (tick!)
        log-tuple (merge tuple { :tx timestep :retracted false })]
  (execute
    (sql/safe-format
      "insert into %s (%s) values (%s)"
      (:log-name relation)
      (sql/safe-infix ", " (keys log-tuple))
      (sql/safe-infix ", " (vals log-tuple))))
  (try
    (execute
      (sql/safe-format
        "update %s set %s where %s"
        (:name relation)
        (sql/safe-infix ",\n"
          (map (fn [[k v]] (sql/sql-= k v)) (select-keys tuple (keys (value-fields relation)))))
        (sql/safe-infix ", "
          (map (fn [[k v]] (sql/sql-= k v)) (select-keys tuple (keys (key-fields relation)))))
        ))
    (catch SQLException e
      (execute
        (sql/safe-format
          "insert into %s (%s) values (%s)"
          (:name relation)
          (sql/safe-infix ", " (keys tuple))
          (sql/safe-infix ", " (vals tuple))))))))

(defn delete! [relation tuple]
  (let [timestep (tick!)
        log-tuple (merge tuple { :tx timestep :retracted true })]
  (execute
    (sql/safe-format
      "insert into %s (%s) values (%s)"
      (:log-name relation)
      (sql/safe-infix ", " (keys log-tuple))
      (sql/safe-infix ", " (vals log-tuple))))
  (execute
    (sql/safe-format
      "delete from %s where %s"
      (:name relation)
      (sql/safe-infix ", "
        (map (fn [[k v]] (sql/sql-= k v)) (select-keys tuple (keys (key-fields relation)))))))))

(defn quondam [relation timestep]
  (remove :retracted
    (execute
      (sql/safe-format
        "select %s, first(retracted order by tx desc) as retracted from %s where %s group by %s"
        (sql/safe-infix ", "
          (map #(sql/safe-format "first(%s order by tx desc) as %s" % %) (keys (fields relation))))
        (:log-name relation)
        (sql/sql-<= :tx timestep)
        (sql/safe-infix ", " (keys (key-fields relation)))))))

;; hotel_to_location (char(24)? or varchar)
;; primary key hotel_tid

;; hotel_to_partner_hotel
;; primary key partner, partner_hotel_identifier
;; still want a unique constraint on hotel_tid (lives on table, need to add tx to all constraints for log table)

;; basics: simple types int, varchar, numeric, float, json, etc.
;; add a mapping from fields to clojure data structures
;; be able to serialize as json (including id attribute for ember)

;; third step is update state set current_tx
;; but cannot insert or delete facts b/c current_tx != last_tx

;; rebase
;; deleting history: move base records into table_staging, then delete with tx <= cutoff, then update the tx = cutoff + 1, then move the records back?
