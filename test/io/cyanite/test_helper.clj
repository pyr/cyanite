(ns io.cyanite.test-helper
  (:require [com.stuartsierra.component :as component]
            [spootnik.reporter          :as reporter]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.drift    :as drift]
            [io.cyanite.engine.queue    :as queue]
            [io.cyanite.pool            :as pool]
            [io.cyanite.store           :as store]
            [io.cyanite.index           :as index]
            [io.cyanite.index.cassandra]))

(defprotocol TimeTraveller
  (set-time! [clock t]))

(defrecord NoOpPool []
  component/Lifecycle
  (start [this] this)
  (stop [this] this)
  pool/Scheduler
  (submit [this f])
  (shutdown [this])
  (shutdown-now [this])
  (join [this])
  (recurring [this f delay]))

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
  (engine-event! [this e]
    (doseq [f @consumers]
      (f e)))
  (consume! [this f]
    (swap! consumers conj f)))

(defrecord SynchronousQueue []
  component/Lifecycle
  (start [this]
    (assoc this :ingestq (SynchronousQueueEngine. (atom []))))
  (stop [this]
    (assoc this :ingestq nil)))

(def ^:dynamic *system*)

(defn make-test-system
  [config overrides]
  (-> {:reporter (reporter/make-reporter {})
       :clock    (map->TimeTravellingClock {})
       ;; No-op compoments
       :pool     (map->NoOpPool {})
       :drift    (drift/map->NoOpDrift {})
       :index    (index/build-index (or (:index config) {:type :atom}))
       :queues   (map->SynchronousQueue {})
       :store    (store/build-store (or (:store config) {:type :memory}))
       ;; Default versions
       :engine   (engine/make-engine (:engine config))
       }
      (merge overrides)
      (component/map->SystemMap)
      (component/system-using {:drift  [:clock]
                               :queues [:reporter]
                               :pool   [:reporter]
                               :index  []
                               :engine [:drift :store :queues :reporter :index]
                               })))


(defmacro with-config
  [config overrides & body]
  `(let [cfg# ~config]
    (if (vector? cfg#)
      (doseq [config# cfg#]
        (binding [*system* (component/start-system (make-test-system config# ~overrides))] ;;TODO: get rid of overrides, wtf
          ~@body
          (component/stop-system *system*)))
      (binding [*system* (component/start-system (make-test-system ~config ~overrides))]
        ~@body
        (component/stop-system *system*))))
  )
