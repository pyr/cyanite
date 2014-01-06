(ns so.grep.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [aleph.tcp             :as tcp]
            [clojure.string        :as s]
            [so.grep.cyanite.store :as store]
            [clojure.tools.logging :refer [info debug]]
            [gloss.core            :refer [string]]
            [lamina.core           :refer [receive-all map* siphon]]))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [rollups input]
  (let [[path metric time] (s/split input #" ")]
    (when (not= metric "nan")
      (for [{:keys [rollup period rollup-to]} rollups]
        {:path   path
         :rollup rollup
         :period period
         :ttl    (* period rollup)
         :time   (rollup-to (Long/parseLong time))
         :metric (Double/parseDouble metric)}))))

(defn handler
  "Send each metric over to the cassandra store"
  [rollups store]
  (fn [ch info]
    (siphon (map* (partial formatter rollups) ch)
            (store/channel-for store))))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store carbon]}]
  (let [handler (handler (:rollups carbon) store)]
    (info "starting carbon handler")
    (tcp/start-tcp-server
     handler
     (merge carbon {:frame (string :utf-8 :delimiters ["\r\n" "\n"])}))))
