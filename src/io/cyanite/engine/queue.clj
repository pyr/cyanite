(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite.
   This is how we "
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
  (consume! [this k opts f])
  (add! [this k e]))

(defn make-event-factory
  [ctor]
  (reify EventFactory
    (newInstance [this]
      (ctor))))

(defprotocol WrapperEvent
  (get-data [_])
  (set-data [this v]))

;; For sakes of prototyping, we are using the mutable
;; wrapper. This is not very idiomatic to ring buffer,
;; so it's better to use mutable bags of primitives.
;; If the approach with RB proves itself, we will migrate
;; the code to use it.
(deftype Event [^{:volatile-mutable true} x]
  WrapperEvent
  (get-data [_] x)
  (set-data [this v] (set! x v)))

(defn wrapper-event-factory
  []
  (make-event-factory
   (fn [] (Event. nil))))

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defn pool-future-call
  [p f]
  (.submit p f))

(defmacro with-future
  [p & body]
  `(pool-future-call ~p (fn [] ~@body)))

(defn disruptor
  [event-factory size executor]
  (Disruptor. event-factory size executor))

(defn make-translator
  [f]
  (reify EventTranslatorOneArg
    (translateTo [this event sequence arg0]
      (f event arg0))))

(defn linked-queue
  [sz]
  (java.util.concurrent.ArrayBlockingQueue.
   (int sz)))

(defrecord BlockingMemoryQueue [state defaults]
  component/Lifecycle
  (start [this]
    (assoc this
           :state (atom {})
           ))
  (stop [this]
    (doseq [[_ {:keys [[pool disruptor]]}] @state]
      (.shutdown disruptor)
      (.shutdown pool))
    (assoc this :state nil))
  QueueEngine
  (consume! [this k opts f]
    (let [sz   (or (:pool-size opts)
                   (get-in defaults [k :pool-size])
                   default-poolsize)
          pool (threadpool sz)
          cap       (or (:queue-capacity opts)
                        (get-in defaults [k :queue-capacity])
                        default-capacity)
          disruptor (disruptor (wrapper-event-factory) cap pool)]
      (swap! state assoc k {:disruptor  disruptor
                            :translator (make-translator (fn [e v]  (set-data e v)))})
      (.handleEventsWith disruptor
                         (into-array [(reify EventHandler
                                         (onEvent [this event sequence eob]
                                           (f (get-data event))))]))
      (.start disruptor)))
  (add! [this k e]
    ;; TODO: dereferencing is an expensive operation going through the atomic memory barrier
    (let [{:keys [disruptor translator]} (get @state k)]
      (.publishEvent disruptor translator e))))
