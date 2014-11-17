(ns io.cyanite
  "Main cyanite namespace"
  (:gen-class)
  (:require [io.cyanite.http   :as http]
            [io.cyanite.config :as config]
            [io.cyanite.index  :as index]
            [io.cyanite.store  :as store]
            [io.cyanite.engine :refer [engine start!]]
            [clojure.tools.logging :refer [debug]]
            [clojure.tools.cli :refer [cli]]))

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

(defn -main
  "Our main function, parses args and launches appropriate services"
  [& args]
  (let [[{:keys [path help quiet]} args banner] (get-cli args)]
    (when help
      (println banner)
      (System/exit 0))
    (let [{:keys [http] :as config} (config/init path quiet)
          config                    (-> config
                                        (update :store store/wrapped-store
                                                (:precisions config))
                                        (update :index index/wrapped-index))
          _                         (debug "ready to start")
          core                      (engine config)]
      (debug "engine built, starting transports")
      (doseq [{:keys [constructor] :as opts} (:transports config)
              :let [transport (constructor opts core)]]
        (debug "starting transport: " (pr-str (dissoc opts :constructor)))
        (conj! core transport))
      (start! core)
      (when (:enabled http)
        (http/server config))))
  nil)
