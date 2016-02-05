(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite."
  (:import java.util.concurrent.Executors
           java.util.ArrayList
           com.lmax.disruptor.RingBuffer
           com.lmax.disruptor.dsl.Disruptor
           com.lmax.disruptor.EventFactory
           com.lmax.disruptor.EventTranslatorOneArg
           com.lmax.disruptor.EventHandler)
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging      :refer [warn]]
            [metrics.counters           :refer [defcounter inc! dec!]]))

(defcounter cnt-queue)
(defcounter cnt-errors)

(defonce default-poolsize 4)
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

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defn disruptor
  [event-factory size executor]
  (Disruptor. event-factory (int size) executor))

(defn make-translator
  [f]
  (reify EventTranslatorOneArg
    (translateTo [this event sequence arg0]
      (f event arg0))))

(defn event-handler
  [f]
  (reify EventHandler
    (onEvent [this event sequence eob]
      (f @event))))

(defrecord DisruptorQueue [disruptor translator]
  QueueEngine
  (shutdown! [this]
    (.shutdown disruptor))
  (add! [this e]
    (.publishEvent disruptor translator e))
  (consume! [this f]
    (.handleEventsWith disruptor (into-array EventHandler [(event-handler f)]))
    (.start disruptor)))

(defn make-queue
  [defaults]
  (let [capacity (or (:queue-capacity defaults) default-capacity)
        pool     (threadpool (or (:pool-size defaults) default-poolsize))
        factory  (make-event-factory #(volatile! nil))]
    (DisruptorQueue. (disruptor factory capacity pool)
                     (make-translator vreset!))))

(defrecord BlockingMemoryQueue [ingestq writeq defaults]
  component/Lifecycle
  (start [this]
    (assoc this :ingestq (make-queue defaults) :writeq  (make-queue defaults)))
  (stop [this]
    (shutdown! ingestq)
    (shutdown! writeq)
    (assoc this :ingestq nil :writeq nil)))
