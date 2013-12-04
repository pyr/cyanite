(ns so.grep.cyanite.config
  (:require [clj-yaml.core         :refer [parse-string]]
            [clojure.tools.logging :refer [error info debug]]))

(def default-logging
  {:use "so.grep.cyanite.logging/start-logging"
   :pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"
   :overrides {:so.grep "debug"}})

(def default-store
  {:use "so.grep.cyanite.store/cassandra-metric-store"})

(def default-carbon
  {:enabled true
   :host    "127.0.0.1"
   :port    2003})

(def default-http
  {:enabled true
   :host    "127.0.0.1"
   :port    8080})

(defn assoc-rollup-to
  [rollups]
  (map (fn [{:keys [period] :as rollup}]
         (assoc rollup :rollup-to #(-> % (quot period) (* period))))
       rollups))

(defn find-ns-var
  [s]
  (try
    (let [n (namespace (symbol s))]
      (require (symbol n))
      (find-var (symbol s)))
    (catch Exception _
      nil)))

(defn instantiate
  "Find a symbol pointing to a function of a single argument and
   call it"
  [class config]
  (if-let [f (find-ns-var class)]
    (f config)
    (throw (ex-info (str "no such namespace: " class) {}))))

(defn get-instance
  [{:keys [use] :as config} target]
  (debug "building " target " with " use)
  (instantiate (-> use name symbol) config))

(defn load-path
  "Try to find a pathname, on the command line, in
   system properties or the environment and load it."
  [path]
  (-> (or path
          (System/getProperty "cyanite.configuration")
          (System/getenv "CYANITE_CONFIGURATION")
          "/etc/cyanite.yaml")
      slurp
      parse-string))

(defn init
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (-> (load-path path)
        (update-in [:logging] (partial merge default-logging))
        (update-in [:logging] get-instance :logging)
        (update-in [:store] (partial merge default-store))
        (update-in [:store] get-instance :store)
        (update-in [:carbon] (partial merge default-carbon))
        (update-in [:carbon :rollups] assoc-rollup-to)
        (update-in [:http] (partial merge default-http)))))
