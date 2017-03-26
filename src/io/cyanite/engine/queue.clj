(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite."
  (:import java.util.concurrent.Executors
           java.util.ArrayList
           org.jctools.queues.SpscArrayQueue
           org.jctools.queues.SpmcArrayQueue
           java.util.concurrent.locks.LockSupport
           )
  (:require [com.stuartsierra.component :as component]
            [spootnik.reporter          :as r]
            [clojure.tools.logging      :refer [warn error]]))


(defonce workers 4)
(defonce default-capacity (int 1048576))

(defprotocol QueueEngine
  (shutdown! [this])
  (start! [this])
  (consume! [this f])
  (engine-event! [this e])
  )

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defrecord EngineQueue [alias reporter pool engine-queues]
  Object
  (toString [_]
    "EngineQueue")
  QueueEngine
  (start! [this]
    ;; no-op
    )
  (shutdown! [this]
    ;; no-op
    )
  (engine-event! [this e]
    (r/inc! reporter [:cyanite alias :events :ingested])
    ;; TODO: implement Round-robin for more fair scheduling?
    (let [queue ^SpscArrayQueue (nth engine-queues (mod (hash e) workers))]
      (.add queue e)))
  (consume! [this f]
    (doseq [queue engine-queues]
      (.submit pool
               (fn []
                 (loop []
                   (try
                     (if-let [el (.poll ^SpscArrayQueue queue)]
                       (f el)
                       (LockSupport/parkNanos 10))
                     (catch Throwable exception
                       (r/inc! reporter [:cyanite alias :events :error])
                       (error exception "exception while processing the event from the queue")))
                   (recur)
                   ))
               ))))

(defn make-queue
  [options alias reporter]
  (let [capacity (or (:queue-capacity options) default-capacity)
        workers  (or (:pool-size options) workers)
        pool     (threadpool workers)]
    (EngineQueue. alias
                  reporter
                  (take workers (repeatedly #(SpscArrayQueue. capacity)))
                  (SpmcArrayQueue. capacity))))

(defrecord BlockingMemoryQueue [ingestq options reporter]
  component/Lifecycle
  (start [this]
    (r/build! reporter :counter [:cyanite :ingestq :events])
    (r/build! reporter :counter [:cyanite :ingestq :errors])
    (assoc this :ingestq (make-queue options :ingestq reporter)))
  (stop [this]
    (shutdown! ingestq)
    (assoc this :ingestq nil)))
