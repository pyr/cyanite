(ns io.cyanite.engine.writer
  "The core of cyanite"
  (:require [io.cyanite.engine          :as engine]
            [com.stuartsierra.component :as component]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [spootnik.reporter          :as r]
            [io.cyanite.pool            :refer [with-schedule set-thread-name!]]
            [clojure.tools.logging      :refer [info debug]]))

(defrecord Writer [index store engine pool reporter]
  component/Lifecycle
  (start [this]
    (info "starting writer engine")
    (doseq [{:keys [pattern resolutions]} (:planner engine)]
      (doseq [{:keys [precision]} resolutions]
        (with-schedule [pool precision]
          (info "starting snapshot.")
          (set-thread-name! "cyanite-snapshot")
          (engine/snapshot-by! this #(re-matches pattern %)))))
    this)
  (stop [this]
    this)
  engine/Snapshoter
  (snapshot! [this]
    (engine/snapshot-by! this (constantly true)))
  (snapshot-by! [this matcher]
    (doseq [metric (engine/snapshot-by! engine matcher)]
      (engine/ingest! this metric)))
  engine/Ingester
  (ingest! [this metric]
    (r/time! reporter
             [:cyanite :writer :indexing]
             (index/register! index (:path metric)))
    (r/time! reporter
             [:cyanite :writer :storage]
             (store/insert! store metric))))
