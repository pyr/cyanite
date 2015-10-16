(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.engine.buckets  :as b]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now!]]
            [io.cyanite.engine.drift    :refer [drift! skewed-epoch!]]
            [clojure.tools.logging      :refer [info debug]]
            [metrics.timers             :refer [deftimer time!]]
            [metrics.core               :refer [new-registry]]
            [metrics.counters           :refer [inc! dec! defcounter]]))

(deftimer t-tuple)
(deftimer t-mkey)
(deftimer t-snap)
(deftimer t-assc)
(deftimer t-enq)

(defprotocol Acceptor
  (accept! [this metric]))

(defprotocol Resolutionator
  (resolution [this oldest path]))

(defn ingest-at-resolution
  [drift buckets queues resolution metric]
  (let [k          (time! t-tuple (b/tuple resolution (:path metric)))
        metric-key (time! t-mkey (or (get buckets k) (b/metric-key k)))
        snaps      (time! t-snap (b/add-then-snap! metric-key metric
                                                   (skewed-epoch! drift)))]
    (time! t-assc
           (assoc-if-absent! buckets k metric-key))
    (doseq [snapshot snaps]
      (time! t-enq (q/add! queues :writeq snapshot)))))

(defrecord Engine [rules planner buckets index store queues drift]
  component/Lifecycle
  (start [this]
    (let [buckets (nbhm)
          planner (map rule/->rule rules)]
      (info "starting engine")
      (q/consume! queues :ingestq {}
                  (fn [metric]
                    (drift! drift (:time metric))
                    (let [plan (rule/->exec-plan planner metric)]
                      (doseq [resolution plan]
                        (ingest-at-resolution drift buckets queues
                                              resolution metric)))))

      (q/consume! queues :writeq {}
                  (fn [metric]
                    (index/register! index (:path metric))
                    (store/insert! store metric)))
      (assoc this :planner planner :bucket buckets)))
  (stop [this]
    (when queues
      (q/stop! queues :ingestq)
      (q/stop! queues :writeq))
    (assoc this :planner nil :buckets nil))
  Acceptor
  (accept! [this metric]
    (q/add! queues :ingestq metric))
  Resolutionator
  (resolution [this oldest path]
    (let [plan (rule/->exec-plan planner {:path path})
          ts   (now!)]
      (when-let [resolution (some #(rule/fit? % oldest ts)
                                  (sort-by :precision plan))]
        {:path path :resolution resolution}))))
