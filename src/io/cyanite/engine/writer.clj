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
    (let [task (with-schedule [pool 10]
                 (set-thread-name! "cyanite-snapshot")
                 (info "starting snapshot.")
                 (engine/snapshot! this))]
      (assoc this :task task)))
  (stop [this]
    this)
  engine/Snapshoter
  (snapshot! [this]
    (doseq [metric (engine/snapshot! engine)]
      (engine/ingest! this metric)))
  engine/Ingester
  (ingest! [this metric]
    (index/register! index (:path metric))
    (store/insert! store metric)))
