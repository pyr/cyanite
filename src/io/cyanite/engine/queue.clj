(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite.
   This is how we "
  (:import java.util.concurrent.Executors
           java.util.ArrayList)
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

(defn threadpool
  [sz]
  (Executors/newFixedThreadPool (int sz)))

(defn pool-future-call
  [p f]
  (.submit p f))

(defmacro with-future
  [p & body]
  `(pool-future-call ~p (fn [] ~@body)))

(defn linked-queue
  [sz]
  (java.util.concurrent.ArrayBlockingQueue.
   (int sz)))

(defrecord BlockingMemoryQueue [state defaults]
  component/Lifecycle
  (start [this]
    (assoc this :state (atom {})))
  (stop [this]
    (if-let [pool (:pool @state)]
      (.shutdown (:pool @state)))
    (assoc this :state nil))
  QueueEngine
  (consume! [this k opts f]
    (let [sz   (or (:pool-size opts)
                   (get-in defaults [k :pool-size])
                   default-poolsize)
          cap  (or (:queue-capacity opts)
                   (get-in defaults [k :queue-capacity])
                   default-capacity)
          pool (threadpool sz)
          q    (linked-queue cap)]
      (swap! state assoc k {:pool pool :queue q})
      (dotimes [i sz]
        (with-future pool
          (loop [elem (.take q)]
            (dec! cnt-queue)
            (try
              (f elem)
              (catch Exception e
                (warn e "could not process element on queue" i "for" k)
                (warn (:exception (ex-data e)) "original exception")))
            (if-not (Thread/interrupted)
              (recur (.take q))))))))
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
