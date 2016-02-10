(ns io.cyanite.test-helper
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.drift    :as drift]
            [io.cyanite.engine.queue    :as queue]
            [io.cyanite.index           :as index]))

(defrecord MemoryWriter [state]
  component/Lifecycle
  (start [this]
    (assoc this :state (atom [])))
  (stop [this] this)
  clojure.lang.IDeref
  (deref [this]
    @state)
  engine/Acceptor
  (accept! [this metric]
    (swap! state conj metric)))

(defprotocol TimeTraveller
  (set-time! [clock t]))

(defrecord TimeTravellingClock [time]
  component/Lifecycle
  (start [this]
    (assoc this :time (atom 0)))
  (stop [this] this)
  drift/Clock
  (epoch! [this]
    (quot @time 1000))
  TimeTraveller
  (set-time! [this t]
    (reset! time t)))

(defrecord SynchronousQueueEngine [consumers]
  queue/QueueEngine
  (shutdown! [this])
  (add! [this e]
    (doseq [f @consumers]
      (f e)))
  (consume! [this f]
    (swap! consumers conj f)))

(defrecord SynchronousQueue []
  component/Lifecycle
  (start [this]
    (assoc this
           :ingestq (SynchronousQueueEngine. (atom []))
           :writeq (SynchronousQueueEngine. (atom []))))
  (stop [this]
    (assoc this :consumers nil)))

(def ^:dynamic *system*)

(defn make-test-system
  [config]
  (-> config
      (update :clock  #(map->TimeTravellingClock %))
      (update :drift  #(component/using (drift/build-drift %) [:clock]))
      (update :queues map->SynchronousQueue)
      (update :index  index/build-index)))


(defmacro with-config
  [config & body]
  `(binding [*system* (component/start-system (make-test-system ~config))]
     ~@body
     (component/stop-system *system*)))
