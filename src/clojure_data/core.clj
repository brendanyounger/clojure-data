(ns clojure-data.core
  (import [java.sql BatchUpdateException PreparedStatement ResultSet SQLException Statement]
          [javax.sql DataSource]
          [com.mchange.v2.c3p0 ComboPooledDataSource]))

; (def db-spec
;   { :classname "org.postgresql.Driver"
;     :subprotocol "postgresql"
;     :subname "//localhost/postgres"
;   })

; (def db (delay (pooled-datasource db-spec)))

(defn pooled-datasource
  [{:keys [classname subprotocol subname user password]}]
  (doto  (ComboPooledDataSource.)
         (.setDriverClass classname)
         (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
         (.setUser user)
         (.setPassword password)))

(defn- safe-resultset-seq [^ResultSet resultset]
  (doall (resultset-seq resultset)))

(defn- make-prepared-statement [connection sql params]
  (let [statement (.prepareStatement connection sql)]
    (dorun
      (map-indexed
        (fn [idx param]
          (.setObject statement (inc idx) param))
        params))
    statement))

;; maybe accept a seq of fragments?
(defn execute [datasource fragment]
  (let [connection  (.getConnection datasource)
        statement   (make-prepared-statement  connection
                                              (:sql fragment)
                                              (:parameters fragment))
        resultset?  (.execute statement)
        value       (if resultset?
                        (safe-resultset-seq (.getResultSet statement))
                        (.getUpdateCount statement))]
    (.close connection)
    value))
