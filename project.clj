(defproject clojure-data "0.1.5-SNAPSHOT"

  :description "Clojure-data is a simple, straightforward library for handling SQL, database connections, and data typing in Clojure"

  :url "https://github.com/brendanyounger/clojure-data"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.mchange/c3p0 "0.9.2.1"]
                 [org.xerial/sqlite-jdbc "3.7.2"]
                 [postgresql/postgresql "9.1-901-1.jdbc4"]
                 ;; [mysql/mysql-connector-java "5.1.6"]
                 [com.taoensso/timbre "1.6.0"]
                 [clojure-csv "2.0.0-alpha2"]
                 [instaparse "1.0.1"]])
