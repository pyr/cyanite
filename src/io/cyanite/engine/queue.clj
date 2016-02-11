(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite."
  (:import java.util.concurrent.Executors
           java.util.ArrayList
           com.lmax.disruptor.RingBuffer
           com.lmax.disruptor.dsl.Disruptor
           com.lmax.disruptor.EventFactory
           com.lmax.disruptor.EventTranslatorOneArg
           com.lmax.disruptor.ExceptionHandler
           com.lmax.disruptor.EventHandler)
  (:require [com.stuartsierra.component :as component]
            [spootnik.reporter          :as r]
            [clojure.tools.logging      :refer [warn error]]
            [metrics.counters           :refer [defcounter inc! dec!]]))


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

(defn exception-handler
  [f]
  (reify ExceptionHandler
    (handleEventException [this ex sequence event]
      (f [ex event]))
    (handleOnShutdownException [this ex]
      ;; no-op
      )
    (handleOnStartException [this ex]
      ;; no-op
      )))

(defrecord DisruptorQueue [disruptor translator alias reporter]
  QueueEngine
  (shutdown! [this]
    (.shutdown disruptor))
  (add! [this e]
    (.publishEvent disruptor translator e))
  (consume! [this f]
    (.handleEventsWith
     disruptor
     (into-array EventHandler
                 [(event-handler (fn [e]
                                   (r/inc! reporter [:cyanite alias :events])
                                   (f e)))]))
    (.handleExceptionsWith
     disruptor
     (exception-handler (fn [[ex event]]
                          (r/inc! reporter [:cyanite alias :errors])
                          (r/capture! reporter ex))))
    (.start disruptor)))

(defn make-queue
  [defaults alias reporter]
  (let [capacity (or (:queue-capacity defaults) default-capacity)
        pool     (threadpool (or (:pool-size defaults) default-poolsize))
        factory  (make-event-factory #(volatile! nil))]
    (DisruptorQueue. (disruptor factory capacity pool)
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
