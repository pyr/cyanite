(ns io.cyanite.engine.queue
  "Queueing mechanism at the heart of cyanite.
   This is how we "
  (:require [com.climate.claypoole :as cp]
            [clojure.tools.logging :refer [warn]]))

(defonce default-poolsize 4)

(defprotocol QueueEngine
  (consume! [this k opts f])
  (add! [this k e])
  (stop! [this k]))

(defn linked-queue
  [opts]
  (java.util.concurrent.ArrayBlockingQueue.
   (int (or (:queue-capacity opts) 10e3))))

(defn queue-engine
  [opts]
  (let [state (atom {})]
    (reify QueueEngine
      (consume! [this k opts f]
        (let [sz   (or (:pool-size opts) default-poolsize)
              pool (cp/threadpool sz)
              q    (linked-queue opts)]
          (swap! state assoc k {:pool pool :queue q})
          (dotimes [i sz]
            (cp/future
              pool
              (loop [elem (.take q)]
                (try
                  (f elem)
                  (catch Exception e
                    (warn e "could not process element on queue" i "for" k)
                    (warn (:exception (ex-data e)) "original exception")))
                (recur (.take q)))))))
      (add! [this k e]
        (if-let [q (get-in @state [k :queue])]
          (.add q e)
          (throw (ex-info "cannot find queue" {:queue-name k}))))
      (stop! [this k]
        :noop))))
