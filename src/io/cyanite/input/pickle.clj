(ns io.cyanite.input.pickle
  (:require [io.cyanite.engine     :as engine]
            [io.cyanite.input.tcp  :as tcp]
            [pickler.parser        :as pickle]
            [clojure.tools.logging :refer [warn]])
  (:import io.netty.handler.codec.LengthFieldBasedFrameDecoder
           io.netty.handler.timeout.ReadTimeoutHandler
           io.netty.util.CharsetUtil))

(defn pickle-lines
  [^io.netty.buffer.ByteBuf input]
  (try
    (let [sz  (.capacity input)
          bb  (.nioBuffer input 0 sz)
          ast (pickle/raw->ast bb)]
      (for [[val time path] (pickle/ast->metrics ast)]
        {:path   path
         :metric (if (string? val) (Double/parseDouble val) val)
         :time   (Long. time)}))
    (catch Exception e
      (warn e "cannot deserialize pickle")
      nil)))

(defn pipeline
  [^Integer read-timeout engine]
  [#(LengthFieldBasedFrameDecoder. Integer/MAX_VALUE 0 4 0 4)
   #(ReadTimeoutHandler. read-timeout)
   (tcp/with-input input
     (doseq [metric (pickle-lines input)]
       (engine/accept! engine metric)))])
