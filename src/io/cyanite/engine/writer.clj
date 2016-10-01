(ns io.cyanite.engine.writer
  "The core of cyanite"
  (:import [java.util.concurrent Executors])
  (:require [io.cyanite.engine          :as engine]
            [com.stuartsierra.component :as component]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.store           :as store]
            [spootnik.reporter          :as r]
            [io.cyanite.pool            :refer [with-schedule set-thread-name!]]
            [clojure.tools.logging      :refer [info debug]]))

(defrecord Writer [store engine pool reporter queues]
  component/Lifecycle
  (start [this]
    (info "starting writer engine")
    (let [executor (Executors/newSingleThreadExecutor)
          queue   (:ingestq queues)]
      (doseq [{:keys [pattern resolutions]} (:planner engine)]
        (doseq [{:keys [precision]} resolutions]
          (with-schedule [pool precision]
            (info "starting snapshot.")
            (set-thread-name! "cyanite-snapshot")
            (q/writer-event! queue
                             ;; TODO: rename, wtf is m
                             (fn [] (engine/snapshot-by! this #(re-matches pattern %))))
            ))))
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
             [:cyanite :writer :storage :single]
             (store/insert! store metric))))
