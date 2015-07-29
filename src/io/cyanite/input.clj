(ns io.cyanite.input
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.engine          :as engine]
            [io.cyanite.input.carbon    :as carbon]
            [io.cyanite.input.pickle    :as pickle]
            [io.cyanite.input.tcp       :refer [map->TCPInput]]
            [io.cyanite.utils           :refer [now!]]
            [clojure.tools.logging      :refer [info]]))

(defrecord FakeInput [thread engine]
  component/Lifecycle
  (start [this]
    (info "starting input thread")
    (assoc this :thread
           (future
             (loop []
               (info "ticking input thread")
               (engine/accept! engine {:path   "foo.bar"
                                       :metric 1.0
                                       :time   (now!)})
               (Thread/sleep 500)
               (recur)))))
  (stop [this]
    (update this :thread #(do (future-cancel %) nil))))


(defmulti build-input (comp (fnil keyword "carbon") :type))

(defmethod build-input :fake
  [options]
  (component/using (map->FakeInput options) [:engine]))

(defmethod build-input :pickle
  [options]
  (component/using
   (map->TCPInput (assoc options
                         :port (or (:port options) 2004)
                         :pipeline pickle/pipeline))
   [:engine]))

(defmethod build-input :carbon
  [options]
  (component/using
   (map->TCPInput (assoc options
                         :port (or (:port options) 2003)
                         :pipeline carbon/pipeline))
   [:engine]))
