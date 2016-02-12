(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite."
  (:import java.util.concurrent.Executors
           java.util.ArrayList
           com.lmax.disruptor.RingBuffer
           com.lmax.disruptor.Sequence
           com.lmax.disruptor.EventFactory
           com.lmax.disruptor.EventTranslatorOneArg
           com.lmax.disruptor.EventHandler
           com.lmax.disruptor.BatchEventProcessor
           com.lmax.disruptor.util.DaemonThreadFactory)
  (:require [com.stuartsierra.component :as component]
            [spootnik.reporter          :as r]
            [clojure.tools.logging      :refer [warn error]]
            [metrics.counters           :refer [defcounter inc! dec!]]))


(defonce default-capacity (int 1024))

(defprotocol QueueEngine
  (shutdown! [this])
  (consume! [this f])
  (add! [this e]))

(defn make-event-factory
  [ctor]
  (reify EventFactory
    (newInstance [this]
      (ctor))))

(defn ring-buffer
  [event-factory size]
  (RingBuffer/createMultiProducer event-factory (int size)))

(defn make-translator
  [f]
  (reify EventTranslatorOneArg
    (translateTo [this event sequence arg0]
      (f event arg0))))

(defn batch-processor
  [ring-buffer barrier handler]
  (BatchEventProcessor. ring-buffer barrier handler))

(defn event-handler
  [f]
  (reify EventHandler
    (onEvent [this event sequence eob]
      (f @event))))

(defrecord DisruptorQueue [executor ring-buffer translator alias reporter]
  QueueEngine
  (shutdown! [this]
    (.shutdown executor))
  (add! [this e]
    (.publishEvent ring-buffer translator e))
  (consume! [this f]
    (let [barrier         (.newBarrier ring-buffer (into-array Sequence []))
          handler         (event-handler
                           (fn [e]
                             (try
                               (r/inc! reporter [:cyanite alias :events])
                               (f e)
                               (catch Exception ex
                                 (r/inc! reporter [:cyanite alias :errors])
                                 (r/capture! reporter ex)))))
          event-processor (batch-processor ring-buffer
                                           barrier
                                           handler)]
      (.submit executor event-processor))))

(defn make-queue
  [defaults alias reporter]
  (let [capacity  (or (:queue-capacity defaults) default-capacity)
        factory   (make-event-factory #(volatile! nil))
        rb        (ring-buffer factory capacity)
        executor  (Executors/newSingleThreadExecutor DaemonThreadFactory/INSTANCE)]
    (DisruptorQueue. executor
                     rb
                     (make-translator vreset!)
                     alias
                     reporter)))

(defrecord BlockingMemoryQueue [ingestq defaults reporter]
  component/Lifecycle
  (start [this]
    (r/build! reporter :counter [:cyanite :ingestq :events])
    (r/build! reporter :counter [:cyanite :ingestq :errors])
    (assoc this :ingestq (make-queue defaults :ingestq reporter)))
  (stop [this]
    (shutdown! ingestq)
    (assoc this :ingestq nil)))
