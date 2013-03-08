(ns clojure-data.chat
  (require [clojure-data.schema :as schema]))

;; we want several distributed tasks to share communique's

;; variants:
;; slow to respond/propose
;; logs to external service
;; one tries to sanitize and then blows up (or has a kill code)
;; one collects stats/watches for interesting events

;; or do we just have a paxoslease implementation?
