(ns io.cyanite.input.carbon
  (:require [io.cyanite.engine     :as engine]
            [io.cyanite.input.tcp  :as tcp]
            [clojure.string        :refer [split]]
            [clojure.tools.logging :refer [info]])
  (:import io.netty.handler.codec.LineBasedFrameDecoder
           io.netty.handler.codec.string.StringDecoder
           io.netty.handler.timeout.ReadTimeoutHandler
           io.netty.util.CharsetUtil))

(defn parse-line
  [^String line]
  (let [[path metric time & garbage] (split line #"\s+")]
    (cond
      garbage
      (throw (ex-info "invalid carbon line: too many fields" {:line line})
             )
      (not (and (seq path) (seq metric) (seq time)))
      (throw (ex-info "invalid carbon line: missing fields" {:line line}))

      (re-find #"(?i)nan" metric)
      (throw (ex-info "invalid carbon line: NaN metric" {:line line})))
    (let [metric (try (Double. metric)
                      (catch NumberFormatException e
                        (throw (ex-info "invalid metric" {:metric metric}))))
          time   (try (Long. time)
                      (catch NumberFormatException e
                        (throw (ex-info "invalid time" {:time time}))))]
      {:path path :metric metric :time time})))

(defn pipeline
  [^Integer read-timeout engine]
  [#(LineBasedFrameDecoder. 2048)
   (StringDecoder. (CharsetUtil/UTF_8))
   #(ReadTimeoutHandler. read-timeout)
   (tcp/with-input input
     (when (seq input)
       (engine/accept! engine (parse-line input))))])
