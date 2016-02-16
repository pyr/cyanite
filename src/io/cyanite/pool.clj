(ns io.cyanite.pool
  (:require [com.stuartsierra.component :as component]
            [spootnik.reporter          :as r]
            [clojure.tools.logging      :refer [info error]])
  (:import java.util.concurrent.ScheduledThreadPoolExecutor
           java.util.concurrent.Executors
           java.util.concurrent.TimeUnit))

(defn set-thread-name!
  [thread-name]
  (.setName (Thread/currentThread) (name thread-name)))

(defonce secs TimeUnit/SECONDS)

(defprotocol Scheduler
  (submit [this f])
  (shutdown [this])
  (shutdown-now [this])
  (join [this])
  (recurring [this f delay]))

(defn wrapped
  [reporter f]
  (fn []
    (try
      (f)
      (catch InterruptedException e
        (info "caught shutdown, stopping task."))
      (catch java.nio.channels.ClosedByInterruptException e
        (info "caught shutdown in I/O, stopping task."))
      (catch Exception e
        (r/capture! reporter e)))))

(defrecord Pool [pool threads reporter]
  component/Lifecycle
  (start [this]
    (assoc this :pool (ScheduledThreadPoolExecutor. (or threads 10))))
  (stop [this]
    (let [remains (.shutdownNow pool)]
      (info "found" (count remains) "pending tasks during shutdown."))
    (assoc this :pool nil))
  Scheduler
  (submit [this f]
    (.submit pool (wrapped reporter f)))
  (shutdown [this]
    (.shutdown pool))
  (shutdown-now [this]
    (.shutdownNow pool))
  (join [this]
    (while (not (.awaitTermination pool 10000 secs)) nil))
  (recurring [this f delay]
    (.scheduleAtFixedRate pool (wrapped reporter f) delay delay secs)))

(defn cancelled?
  [task]
  (.isCancelled task))

(defn done?
  [task]
  (.isDone task))

(defn stopped?
  [task]
  (or (cancelled? task) (done? task)))

(defmacro with-pool
  [pool & body]
  `(submit ~pool (fn [] ~@body)))

(defmacro with-schedule
  [[pool delay] & body]
  `(recurring ~pool (fn [] ~@body) ~delay))

(defn make-pool
  [config]
  (map->Pool config))
