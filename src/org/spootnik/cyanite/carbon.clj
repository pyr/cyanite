(ns org.spootnik.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [aleph.tcp                  :as tcp]
            [clojure.string             :as s]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.path  :as path]
            [clojure.tools.logging      :refer [info debug]]
            [gloss.core                 :refer [string]]
            [lamina.core                :refer :all]))

(set! *warn-on-reflection* true)

(comment
  (defn form
    [index rollups ^String input]
    (let [[path metric time] (s/split (.trim input) #" ")]
      (when (and (not= metric "nan")
                 time
                 path)
        ;; hardcode empty tenant for now
        (when index (path/register index "" path))
        (for [{:keys [rollup period rollup-to]} rollups]
          {:path   path
           :rollup rollup
           :period period
           :ttl    (* period rollup)
           :time   (rollup-to (Long/parseLong time))
           :metric (Double/parseDouble metric)}))))

  (defn formatter
    "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
    [index rollups ^org.jboss.netty.buffer.ChannelBuffer input]
    (let [sin (String. (.array input))
          splitted (clojure.string/split-lines sin)]
      (map (partial form index rollups) splitted)))

  (defn handler
    "Send each metric over to the cassandra store"
    [index rollups insertch]
    (fn [ch info]
      (try
        (let [c (read-channel ch)]
          (on-realized c
                       #(do
                          (close ch)
                          (doall (map (fn [d] (enqueue insertch d)) (formatter index rollups %))))
                       #(do
                          (close ch)
                          (info "ERROR: " %))))
        (catch Exception e
          (close ch)
          (info "EXCEPTION: " e))))))

(defn devnull
  [index rollups insertch]
  (fn [ch info]
    (close ch)))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [index rollups ^String input]
  (let [[path metric time] (s/split (.trim input) #" ")]
    (when (not= metric "nan")
      ;; hardcode empty tenant for now
      (path/register index "" path)
      (for [{:keys [rollup period rollup-to]} rollups]
        {:path   path
         :rollup rollup
         :period period
         :ttl    (* period rollup)
         :time   (rollup-to (Long/parseLong time))
         :metric (Double/parseDouble metric)}))))

(defn handler
  "Send each metric over to the cassandra store"
  [indexch rollups insertch]
  (fn [ch info]
    (siphon (map* (partial formatter indexch rollups) ch) insertch)))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store carbon index]}]
  (let [insertch (store/channel-for store)
    ;    indexch (path/rchannel index)
        handler  (handler index (:rollups carbon) insertch)]
    (info "starting carbon handler")
    (tcp/start-tcp-server
     handler
     (merge carbon {:frame (string :utf8 :delimiter ["\n"])}))))
