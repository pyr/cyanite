(ns org.spootnik.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [aleph.tcp                  :as tcp]
            [clojure.string             :as s]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.path  :as path]
            [clojure.tools.logging      :refer [info debug]]
            [gloss.core                 :refer [string]]
            [lamina.core                :refer [receive-all map* siphon]]))

(defn parse-num
  "parse a number into the given value, return the
  default value if it fails"
  [parse default number]
  (try (parse number)
    (catch NumberFormatException e
      (debug "got an invalid number" number (.getMessage e))
      default)))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [index rollups input]
  (let [[path metric time] (s/split (.trim input) #" ")
        metric (parse-num #(Double/parseDouble %) "nan" metric)
        time (parse-num #(Long/parseLong %) "nan" time)]
    (when (and (not= metric "nan") (not= time "nan"))
      ;; hardcode empty tenant for now
      (when index (path/register index "" path))
      (for [{:keys [rollup period rollup-to]} rollups]
        {:path   path
         :rollup rollup
         :period period
         :ttl    (* period rollup)
         :time   (rollup-to time)
         :metric metric}))))

(defn handler
  "Send each metric over to the cassandra store"
  [index rollups insertch]
  (fn [ch info]
    (siphon (map* (partial formatter index rollups) ch) insertch)))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store carbon index]}]
  (let [insertch (store/channel-for store)
        handler  (handler index (:rollups carbon) insertch)]
    (info "starting carbon handler")
    (tcp/start-tcp-server
     handler
     (merge carbon {:frame (string :utf-8 :delimiters ["\n"])}))))
