(ns io.cyanite
  "Main cyanite namespace"
  (:gen-class)
  (:require [io.cyanite.config          :as config]
            [io.cyanite.signals         :as sig]
            [io.cyanite.input           :as input]
            [io.cyanite.index           :as index]
            [io.cyanite.index.cassandra]
            [io.cyanite.engine.queue    :as queue]
            [io.cyanite.store           :as store]
            [com.stuartsierra.component :as component]
            [metrics.reporters.console  :as console]
            [metrics.reporters.csv      :as csv]
            [metrics.reporters.jmx      :as jmx]
            [io.cyanite.engine          :refer [map->Engine]]
            [io.cyanite.engine.writer   :refer [map->Writer]]
            [io.cyanite.engine.drift    :refer [map->SystemClock build-drift]]
            [io.cyanite.api             :refer [map->Api]]
            [unilog.config              :refer [start-logging!]]
            [spootnik.uncaught          :refer [uncaught]]
            [clojure.tools.logging      :refer [info warn]]
            [clojure.tools.cli          :refer [cli]]))

(set! *warn-on-reflection* true)

(defn get-cli
  "Call cli parsing with our known options"
  [args]
  (try
    (cli args
         ["-h" "--help" "Show help" :default false :flag true]
         ["-f" "--path" "Configuration file path" :default nil]
         ["-q" "--quiet" "Suppress output" :default false :flag true])
    (catch Exception e
      (binding [*out* *err*]
        (println "Could not parse arguments: " (.getMessage e)))
      (System/exit 1))))

(defn build-components
  "Build a list of components from data.
   Extracts key k from system and yield
   an updated system with top-level keys.
   components are created by call f on them
   options."
  [system k f]
  (if (seq (get system k))
    (merge (dissoc system k)
           (reduce merge {} (map (juxt :type f) (get system k))))
    ;; When key didn't exist in map, create, call the constructor
    ;; on an empty option map
    (assoc system k (f {}))))

(defn config->system
  "Parse yaml then enhance config"
  [path quiet?]
  (try
    (when-not quiet?
      (println "starting with configuration: " path))
    (let [config (config/load-path path)]
      (start-logging! (merge config/default-logging (:logging config)))
      (-> (component/system-map
           :input  (input/build-input (:input config))
           :clock  (map->SystemClock {})
           :queues (queue/map->BlockingMemoryQueue (:queue config))
           :drift  (build-drift (:drift config))
           :engine (map->Engine (:engine config))
           :writer (map->Writer (:writer config))
           :api    (map->Api (:api config))
           :index  (index/build-index (:index config))
           :store  (store/build-store (:store config)))
          (build-components :input input/build-input)
          (component/system-using {:drift [:clock]
                                   :engine [:drift :queues :writer]
                                   :writer [:index :store :queues]
                                   :api    [:index :store :queues :engine]})))))

(defn -main
  "Our main function, parses args and launches appropriate services"
  [& args]
  (let [[{:keys [path help quiet]} args banner] (get-cli args)
        cr (csv/reporter "/tmp/csv" {})
        jr (jmx/reporter {})]

    (csv/start cr 5)
    (jmx/start jr)
    (when help
      (println banner)
      (System/exit 0))

    (let [system (atom (config->system path quiet))]
      (info "installing signal handlers")
      (sig/with-handler :term
        (info "caught SIGTERM, quitting")
        (component/stop-system @system)
        (info "all components shut down")
        (System/exit 0))

      (sig/with-handler :hup
        (info "caught SIGHUP, reloading")
        (swap! system (comp component/start-system
                            component/stop-system)))

      (info "ready to start the system")
      (swap! system component/start-system)))
  nil)

;; Install our uncaught exception handler.
(uncaught e (warn e "uncaught exception"))
