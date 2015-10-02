(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite.
   This is how we "
  (:import java.util.concurrent.Executors
           java.util.ArrayList)
  (:require [clojure.tools.logging :refer [warn]]))

(defonce default-poolsize 4)

(defprotocol QueueEngine
  (consume! [this k opts f])
  (add! [this k e])
  (stop! [this k]))

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
  [opts]
  (java.util.concurrent.ArrayBlockingQueue.
   (int (or (:queue-capacity opts) 10e3))))

(defn drain!
  [q]
  (let [al (ArrayList.)]
    (.drainTo q al)
    al))

(defn queue-engine
  [opts]
  (let [state (atom {})]
    (reify QueueEngine
      (consume! [this k opts f]
        (let [sz   (or (:pool-size opts) default-poolsize)
              pool (threadpool sz)
              q    (linked-queue opts)]
          (swap! state assoc k {:pool pool :queue q})
          (dotimes [i sz]
            (with-future pool
              (loop [elems (drain! q)]
                (try
                  (doseq [elem elems]
                    (f elem))
                  (catch Exception e
                    (warn e "could not process element on queue" i "for" k)
                    (warn (:exception (ex-data e)) "original exception")))
                (recur (drain! q)))))))
      (add! [this k e]
        (if-let [q (get-in @state [k :queue])]
          (.add q e)
          (throw (ex-info "cannot find queue" {:queue-name k}))))
      (stop! [this k]
        :noop))))
