(ns io.cyanite.transport.carbon
  "Dead simple carbon protocol handler"
  (:require [io.cyanite.transport.tcp :as tcp]
            [io.cyanite.engine        :as engine]
            [clojure.core.async       :as a]
            [clojure.tools.logging    :refer [info debug warn]]
            [clojure.string           :refer [split]]))

(set! *warn-on-reflection* true)

(defn time->long
  [^String t]
  (try
    (Long/parseLong t)
    (catch Exception e
      (warn e "invalid time definition " t))))

(defn metric->double
  [^String m]
  (try
    (Double/parseDouble m)
    (catch Exception e
      (warn e "invalid double definition " m))))

(defn text->input
  [^String input]
  (let [[^String path ^String metric ^String time] (split (.trim input) #" ")]
    {:path path
     :metric (metric->double metric)
     :time   (time->long time)}))

(defn start
  "Start a tcp carbon listener"
  [config core]
  (let [chan   (a/chan 100000)
        mapped (a/remove< nil? (a/map< text->input chan))]
    (reify
      engine/Service
      (start! [this]
        (info "starting carbon handler")
        (tcp/start-tcp-server
         (merge config {:response-channel chan})))
      clojure.lang.ILookup
      (valAt [this k]
        (when (= k :channel)
          mapped)))))
