(ns so.grep.cyanite.carbon
  (:require [aleph.tcp             :as tcp]
            [clojure.string        :as s]
            [so.grep.cyanite.store :as store]
            [clojure.tools.logging :refer [info debug]]
            [gloss.core            :refer [string]]
            [lamina.core           :refer [receive-all map* siphon]]))

(defn formatter
  [rollups input]
  (debug "got input: " input)
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
  [rollups carbon]
  (fn [ch info]
    (siphon (map* (partial formatter rollups) ch)
            (store/channel-for carbon))))

(defn start
  [{:keys [store carbon]}]
  (let [handler (handler (:rollups carbon) store)]
    (info "starting carbon handler with config: " carbon)
    (tcp/start-tcp-server
     handler
     (merge carbon {:frame (string :utf-8 :delimiters ["\r\n"])}))))
