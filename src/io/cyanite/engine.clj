(ns io.cyanite.engine
  "The core of cyanite"
  (:require [io.cyanite.engine          :as engine]
            [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.engine.buckets  :as b]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch!]]
            [clojure.tools.logging      :refer [info debug]]
            [metrics.timers             :refer [deftimer time!]]))

(deftimer t-tuple)
(deftimer t-mkey)
(deftimer t-snap)
(deftimer t-assc)
(deftimer t-enq)

(defprotocol Acceptor
  (accept! [this value]))

(defprotocol Resolutionator
  (resolution [this oldest path]))

(defn ingest-at-resolution
  [drift buckets writer resolution metric]
  (let [k          (time! t-tuple (b/tuple resolution (:path metric)))
        metric-key (time! t-mkey (or (get buckets k) (b/metric-key k)))
        snaps      (time! t-snap (b/add-then-snap! metric-key metric
                                                   (skewed-epoch! drift)))]
    (time! t-assc
           (assoc-if-absent! buckets k metric-key))
    (doseq [snapshot snaps]
      (time! t-enq (engine/accept! writer snapshot)))))

(defrecord Engine [rules queues ingestq planner drift writer]
  component/Lifecycle
  (start [this]
    (let [buckets (nbhm)
          planner (map rule/->rule rules)
          ingestq (:ingestq queues)]
      (info "starting engine")
      (q/consume! ingestq
                  (fn [metric]
                    (drift! drift (:time metric))
                    (let [plan (rule/->exec-plan planner metric)]
                      (doseq [resolution plan]
                        (ingest-at-resolution drift buckets writer
                                              resolution metric)))))

      (assoc this :planner planner :bucket buckets :ingestq ingestq)))
  (stop [this])
  engine/Acceptor
  (accept! [this metric]
    (q/add! ingestq metric))
  engine/Resolutionator
  (resolution [this oldest path]
    (let [plan (rule/->exec-plan planner {:path path})
          ts   (now!)]
      (when-let [resolution (some #(rule/fit? % oldest ts)
                                  (sort-by :precision plan))]
        {:path path :resolution resolution}))))
