(ns io.cyanite.engine.writer
  "The core of cyanite"
  (:require [io.cyanite.engine          :as engine]
            [com.stuartsierra.component :as component]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.store           :as store]
            [spootnik.reporter          :as r]
            [io.cyanite.pool            :refer [with-schedule set-thread-name!]]
            [clojure.tools.logging      :refer [info debug]]))

(defrecord Writer [store engine pool reporter batch-size]
  component/Lifecycle
  (start [this]
    (info "starting writer engine")
    (let [self (assoc this
                      :batch-size (or (-> store :options :batch-size) 1))]
      (doseq [{:keys [pattern resolutions]} (:planner engine)]
        (doseq [{:keys [precision]} resolutions]
          (with-schedule [pool precision]
            (info "starting snapshot.")
            (set-thread-name! "cyanite-snapshot")
            (engine/snapshot-by! self #(re-matches pattern %)))))
      self))
  (stop [this]
    this)
  engine/Snapshoter
  (snapshot! [this]
    (engine/snapshot-by! this (constantly true)))
  (snapshot-by! [this matcher]
    (doseq [batch (->> (engine/snapshot-by! engine matcher)
                       (partition batch-size batch-size nil))]
      (if (and (> batch-size 1)
               (= batch-size (count batch)))
        (r/time! reporter
                 [:cyanite :writer :storage :batch]
                 (store/insert-batch! store batch))
        (doseq [metric batch]
          (r/time! reporter
                   [:cyanite :writer :storage :single]
                   (store/insert! store metric))))))
  engine/Ingester
  (ingest! [this metric]
    (throw (RuntimeException. "Not implemented"))
    ;; no-op to avoid indirection in snapshot-by
    ))
