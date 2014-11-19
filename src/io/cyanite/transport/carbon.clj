(ns io.cyanite.transport.carbon
  "Dead simple carbon protocol handler"
  (:require [io.cyanite.transport.tcp :as tcp]
            [io.cyanite.engine        :as engine]
            [org.spootnik.pickler     :as pickler]
            [clojure.core.async       :as a]
            [clojure.tools.logging    :refer [info debug warn]]
            [clojure.pprint           :refer [pprint]]
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
    [{:path path
      :point (metric->double metric)
      :time   (time->long time)}]))

(defn pickle->input
  [^io.netty.buffer.ByteBuf input]
  (try
    (let [sz  (.capacity input)
          bb  (.nioBuffer input 0 sz)
          ast (pickler/raw->ast bb)]
      (for [[val time path] (pickler/ast->metrics ast)]
        {:path  path
         :point (if (string? val) (Double/parseDouble val) val)
         :time  time}))
    (catch Exception e
      (debug e "cannot deserialize pickle")
      nil)))

(defn carbon-transport
  [config core]
  (let [type               (-> config :type keyword)
        timeout            (or (:timeout config) 30)
        ch                 (a/chan (a/dropping-buffer 1000))
        convert            (if (= type :pickle) pickle->input text->input)
        mapped             (a/remove< nil? (a/mapcat< convert ch))]
    (reify
      engine/Service
      (start! [this]
        (info "starting carbon handler " type)
        (tcp/start-tcp-server config ch))
      clojure.lang.ILookup
      (valAt [this k]
        (when (= k :channel)
          mapped)))))
