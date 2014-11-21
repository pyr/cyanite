(ns io.cyanite.config
  "Yaml config parser, with a poor man's dependency injector"
  (:require [clj-yaml.core          :refer [parse-string]]
            [clojure.string         :refer [split]]
            [org.spootnik.logconfig :refer [start-logging!]]
            [io.cyanite.resolution  :refer [any->Resolution]]
            [clojure.tools.logging  :refer [error info debug]]))

(def
  ^{:doc "handle logging configuration from the yaml file"}
  default-logging
  {:pattern "%p [%d] %t - %c - %m%n"
   :external false
   :console true
   :files  []
   :level  "info"
   :overrides {:io.cyanite "debug"}})

(def ^{:doc "handle storage with cassandra-metric-store by default"}
  default-store
  {:use "io.cyanite.store.cassandra/cassandra-store"})

(def ^{:doc "let carbon listen on 2003 by default"}
  default-transport
  {:enabled true
   :use     "io.cyanite.transport.carbon/carbon-transport"
   :host    "127.0.0.1"
   :port    2003})

(def ^{:doc "let the http api listen on 8080 by default"}
  default-http
  {:enabled true
   :host    "127.0.0.1"
   :port    8080})

(def default-index
  {:use "io.cyanite.index.atom/atom-index"})



(defn find-ns-var
  "Find a symbol in a namespace"
  [s]
  (try
    (let [n (namespace (symbol s))]
      (require (symbol n))
      (find-var (symbol s)))
    (catch Exception e
      (prn "Exception: " e))))

(defn instantiate
  "Find a symbol pointing to a function of a single argument.
   If arguments are provided, call the function"
  ([class]
     (or (find-ns-var class)
         (throw (ex-info (str "no such namespace: " class) {}))))
  ([class config args]
     (apply (instantiate class) config args)))

(defn get-instance
  "For dependency injected configuration elements, find build fn
   and call it"
  [{:keys [use] :as config} target & args]
  (debug "building " target " with " use)
  (instantiate (-> use name symbol) config args))

(defn get-sym
  "For dependency injected configuration elements, find build fn
   and call it"
  [{:keys [use type] :as config}]
  (debug "fetching" use)
  (assoc config :constructor (instantiate (-> use name symbol))))

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
  "Parse yaml then enhance config"
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (let [config           (load-path path)
          update-transport (comp
                            get-sym
                            (partial merge default-transport))]

      (start-logging! (merge default-logging (:logging config)))
      (-> config

          (update-in [:store] (partial merge default-store))
          (update-in [:store] get-instance :store)
          (update-in [:resolutions] (partial mapv any->Resolution))
          (update-in [:transports] (partial mapv update-transport))

          (update-in [:index] (partial merge default-index))
          (update-in [:index] get-instance :index)

          (update-in [:http] (partial merge default-http))))))
