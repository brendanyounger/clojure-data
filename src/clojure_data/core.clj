(ns clojure-data.core
  (import [java.sql BatchUpdateException PreparedStatement ResultSet SQLException Statement]
          [javax.sql DataSource]
          [com.mchange.v2.c3p0 ComboPooledDataSource]))

; (def db-spec
;   { :classname "org.sqlite.JDBC"
;     :subprotocol "sqlite"
;     :subname "/Users/brendanyounger/test.db"
;     :user nil
;     :password nil
;   })

(def db-spec
  { :classname "org.postgresql.Driver"
    :subprotocol "postgresql"
    :subname "//localhost/postgres"
  })

(defn pooled-datasource
  [{:keys [classname subprotocol subname user password]}]
  (doto  (ComboPooledDataSource.)
         (.setDriverClass classname)
         (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
         (.setUser user)
         (.setPassword password)))

(def db (delay (pooled-datasource db-spec)))

(defn- safe-resultset-seq [^ResultSet resultset]
  (and resultset (resultset-seq resultset)))

(defn- make-prepared-statement [connection sql params]
  (let [statement (.prepareStatement connection sql)]
    (dorun
      (map-indexed
        (fn [idx param]
          (.setObject statement (inc idx) param))
        params))
    statement))

;; maybe accept a seq of fragments?
(defn execute [fragment]
  (println fragment)
  (let [statement (make-prepared-statement  (.getConnection @db)
                                            (:sql fragment)
                                            (:parameters fragment))
        success   (.execute statement)
        results   (safe-resultset-seq (.getResultSet statement))]
    (or results success)))
