(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite.
   This is how we "
  (:import java.util.concurrent.Executors
           java.util.ArrayList
           com.lmax.disruptor.RingBuffer
           com.lmax.disruptor.dsl.Disruptor
           com.lmax.disruptor.EventFactory
           com.lmax.disruptor.EventHandler)
  (:require [com.stuartsierra.component :as component]
            [clojure.tools.logging      :refer [warn]]
            [metrics.counters           :refer [defcounter inc! dec!]]))

(defcounter cnt-queue)
(defcounter cnt-errors)
(defonce default-poolsize 4)
(defonce default-capacity (int 10e2))

(defprotocol QueueEngine
  (consume! [this k opts f])
  (add! [this k e]))

(defn make-event-factory
  [ctor]
  (reify EventFactory
    (newInstance [this]
      (ctor))))

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defn pool-future-call
  [p f]
  (.submit p f))

(defmacro with-future
  [p & body]
  `(pool-future-call ~p (fn [] ~@body)))

(defn ring-buffer
  [size]
  )
(defn linked-queue
  [sz]
  (java.util.concurrent.ArrayBlockingQueue.
   (int sz)))

(defrecord BlockingMemoryQueue [state pool defaults]
  component/Lifecycle
  (start [this]
    (let [sz   (or (:pool-size opts)
                   (get-in defaults [k :pool-size])
                   default-poolsize)]
      (assoc this
             :state (atom {})
             pool (threadpool sz))))
  (stop [this]
    (.shutdown (:pool @state))
    (assoc this :state nil))
  QueueEngine
  (consume! [this k opts event-ctor f]
    (let [cap  (or (:queue-capacity opts)
                   (get-in defaults [k :queue-capacity])
                   default-capacity)
          rb    (ring-buffer (event-factory event-ctor) cap pool)]
      (swap! state assoc k {:ring-buffer rb})
      (.handleEventsWith rb
                         (reify EventHandler
                           (onEvent [this event sequence eob]
                             (f event))))
      (.start rb)))
  (add! [this k e]
    (if-let [q (get-in @state [k :queue])]
      (do (inc! cnt-queue)
          (try
            (.add q e)
            (catch IllegalStateException e
              ;; Queue is full
              (inc! cnt-errors)
              (warn "queue is full"))))
      (throw (ex-info "cannot find queue" {:queue-name k})))))
