(ns io.cyanite.engine
  "The core of cyanite"
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.engine.buckets  :as b]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now!]]
            [clojure.tools.logging      :refer [info debug]]))

(defprotocol Acceptor
  (accept! [this metric]))

(defprotocol Resolutionator
  (resolution [this oldest path]))

(defn ingest-at-resolution
  [buckets queues resolution metric]
  (let [k          (b/tuple resolution (:path metric))
        metric-key (or (get buckets k) (b/metric-key k))
        snaps      (b/add! metric-key metric)]
    (assoc-if-absent! buckets k metric-key)
    (doseq [snapshot snaps]
      (q/add! queues :writeq (assoc snapshot :resolution resolution)))))

(defrecord Engine [rules planner index store queues]
  component/Lifecycle
  (start [this]
    (let [buckets (nbhm)
          planner (map rule/->rule rules)]
      (info "starting engine")
      (q/consume! queues :ingestq {}
                  (fn [metric]
                    (let [plan (rule/->exec-plan planner metric)]
                      (doseq [resolution plan]
                        (ingest-at-resolution buckets queues
                                              resolution metric)))))

      (q/consume! queues :writeq {}
                  (fn [metric]
                    (debug "in write callback: " (pr-str metric))
                    (index/register! index metric)
                    (store/insert! store metric)))
      (assoc this :planner planner)))
  (stop [this]
    (when queues
      (q/stop! queues :ingestq)
      (q/stop! queues :writeq))
    this)
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
