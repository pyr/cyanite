(ns io.cyanite.engine.writer
  "The core of cyanite"
  (:require [io.cyanite.engine          :as engine]
            [com.stuartsierra.component :as component]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [clojure.tools.logging      :refer [info debug]]))

(defrecord Writer [index store queues writeq]
  component/Lifecycle
  (start [this]
    (info "starting writer engine")
    (let [writeq (:writeq queues)]
      (q/consume! writeq
                  (fn [metric]
                    (index/register! index (:path metric))
                    (store/insert! store metric)))
      (assoc this :writeq writeq)))
  (stop [this]
    (assoc this))
  engine/Acceptor
  (accept! [this metric]
    (q/add! writeq metric)))
