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
            [clojure.tools.logging      :refer [warn error]]
            [metrics.counters           :refer [defcounter inc! dec!]]))

(defcounter cnt-ingestq)
(defcounter cnt-writeq)
(defcounter cnt-errors-ingestq)
(defcounter cnt-errors-writeq)

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

(defrecord DisruptorQueue [disruptor translator cnt error-cnt]
  QueueEngine
  (shutdown! [this]
    (.shutdown disruptor))
  (add! [this e]
    (.publishEvent disruptor translator e))
  (consume! [this f]
    (.handleEventsWith disruptor (into-array EventHandler [(event-handler (fn [e]
                                                                            (inc! cnt)
                                                                            (f e)))]))
    (.handleExceptionsWith disruptor (exception-handler (fn [[ex event]]
                                                          (inc! error-cnt)
                                                          (error ex "could not handle an event"))))
    (.start disruptor)))

(defn make-queue
  [defaults cnt error-cnt]
  (let [capacity (or (:queue-capacity defaults) default-capacity)
        pool     (threadpool (or (:pool-size defaults) default-poolsize))
        factory  (make-event-factory #(volatile! nil))]
    (DisruptorQueue. (disruptor factory capacity pool)
                     (make-translator vreset!)
                     cnt
                     error-cnt)))

(defrecord BlockingMemoryQueue [ingestq writeq defaults]
  component/Lifecycle
  (start [this]
    (assoc this
           :ingestq (make-queue defaults cnt-ingestq cnt-errors-ingestq)
           :writeq  (make-queue defaults cnt-writeq cnt-errors-writeq)))
  (stop [this]
    (shutdown! ingestq)
    (shutdown! writeq)
    (assoc this :ingestq nil :writeq nil)))
