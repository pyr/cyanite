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
(defonce default-capacity (int 1024))

(defprotocol QueueEngine
  (shutdown! [this])
  (start! [this])
  (consume! [this f])
  (engine-event! [this e])
  (writer-event! [this e])
  )

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defrecord EngineQueue [alias reporter pool engine-queues writer-queue]
  Object
  (toString [_]
    "EngineQueue")
  QueueEngine
  (shutdown! [this]
    ;; (.shutdown disruptor)
    )
  (engine-event! [this e]
    (r/inc! reporter [:cyanite alias :events :ingested])
    ;; TODO: implement Round-robin for more fair scheduling?
    (.add (nth engine-queues (mod (hash e) workers)) e)
    ;; (.publishEvent disruptor translator e)
    )

  (writer-event! [this e]
    ;; (r/inc! reporter [:cyanite alias :events :ingested])
    (.add writer-queue e)
    ;; (.publishEvent disruptor translator e)
    )
  (start! [this]
    )
  (consume! [this f]
    (doseq [queue engine-queues]
      (.submit pool
               (fn []
                 (loop []
                   (when-let [f (.poll writer-queue)]
                     ;; TODO: poll-all-available?
                     ;; TODO: exception handling
                     ;; TODO: metrics
                     (f))
                   (if-let [el (.poll queue)]
                     (f el)
                     (LockSupport/parkNanos 10))
                   (recur))
                 ))
      )))

(defn make-queue
  [options alias reporter]
  (let [capacity (or (:queue-capacity options) default-capacity)
        workers  (or (:pool-size options) workers)
        pool     (threadpool workers)]
    (EngineQueue. alias
                  reporter
                  pool
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
