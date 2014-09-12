(ns org.spootnik.cyanite.carbon
  "Dead simple carbon protocol handler"
  (:require [aleph.tcp                  :as tcp]
            [clojure.string             :as s]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.path  :as path]
            [org.spootnik.cyanite.tcp   :as tc]
            [org.spootnik.cyanite.util  :refer [partition-or-time counter-get counters-reset! counter-inc! counter-list]]
            [clojure.tools.logging      :refer [info debug]]
            [gloss.core                 :refer [string]]
            [lamina.core                :refer :all]
            [clojure.core.async :as async :refer [<! >! >!! go chan timeout]]))

(set! *warn-on-reflection* true)

(defn parse-num
  "parse a number into the given value, return the
  default value if it fails"
  [parse default number]
  (try (parse number)
    (catch Exception e
      (debug "got an invalid number" number (.getMessage e))
      default)))

(defn formatter
  "Split each line on whitespace, discard nan metric lines
   and format correct lines for each resolution"
  [rollups ^String input]
  (try
    (let [[path metric time tenant] (s/split (.trim input) #" ")
          timel (parse-num #(Long/parseLong %) "nan" time)
          metricd (parse-num #(Double/parseDouble %) "nan" metric)
          tenantstr (or tenant "NONE")]
      (when (and (not= "nan" metricd) (not= "nan" timel))
          (for [{:keys [rollup period ttl rollup-to]} rollups]
            {:path   path
             :tenant tenantstr
             :rollup rollup
             :period period
             :ttl    (or ttl (* rollup period))
             :time   (rollup-to timel)
             :metric metricd})))
      (catch Exception e
          (info "Exception for metric [" input "] : " e))))

(defn format-processor
  "Send each metric over to the cassandra store"
  [chan indexch rollups insertch]
  (go
    (let [input (partition-or-time 1000 chan 500 5)]
      (while true
        (let [metrics (<! input)]
          (try
            (counter-inc! :metrics_recieved (count metrics))
            (doseq [metric metrics]
              (let [formed (remove nil? (formatter rollups metric))]
                (doseq [f formed]
                  (>! insertch f))
                (doseq [p (distinct (map (juxt :path :tenant) formed))]
                  (>! indexch p))))
            (catch Exception e
              (info "Exception for metric [" metrics "] : " e))))))))

(defn start
  "Start a tcp carbon listener"
  [{:keys [store carbon index stats]}]
  (let [indexch (path/channel-for index)
        insertch (store/channel-for store)
        chan (chan 100000)
        handler (format-processor chan indexch (:rollups carbon) insertch)]
    (info "starting carbon handler: " carbon)
    (go
      (let [{:keys [hostname tenant interval]} stats]
        (while true
          (<! (timeout (* interval 1000)))
          (doseq [[k _]  (counter-list)]
            (>! chan (clojure.string/join " " [(str hostname ".cyanite." (name k))
                                               (counter-get k)
                                               (quot (System/currentTimeMillis) 1000)
                                               tenant])))
          (counters-reset!))))
    (tc/start-tcp-server
     (merge carbon {:response-channel chan}))))
