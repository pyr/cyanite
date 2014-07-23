(ns org.spootnik.cyanite.util
  (:require [clojure.core.async :as async :refer [alt! chan >! close! go
                                                  timeout >!! go-loop
                                                  dropping-buffer]]
            [clojure.tools.logging :refer [info warn error]]))

(defmacro go-forever
  [body]
  `(go
     (while true
       (try
         ~body
         (catch Exception e
           (error e (or (.getMessage e)
                        "Exception while processing channel message")))))))

(defmacro go-catch
  [& body]
  `(go
     (try
       ~@body
       (catch Exception e
         (error e (or (.getMessage e)
                      "Exception while processing channel message"))))))

(defn partition-or-time
  "Returns a channel that will either contain vectors of n items taken from ch or
   if beat-rate millis elapses then a vector with the available items. The
   final vector in the return channel may be smaller than n if ch closed before
   the vector could be completely filled."
  [n ch beat-rate buf-or-n]
  (let [out (chan buf-or-n)]
    (go (loop [arr (make-array Object n)
               idx 0
               beat (timeout beat-rate)]
          (let [[v c] (alts! [ch beat])]
            (if (= c beat)
              (do
                (if (> idx 0)
                  (do (>! out (vec (take idx arr)))
                      (recur (make-array Object n)
                             0
                             (timeout beat-rate)))
                  (recur arr idx (timeout beat-rate))))
              (if-not (nil? v)
                (do (aset ^objects arr idx v)
                    (let [new-idx (inc idx)]
                      (if (< new-idx n)
                          (recur arr new-idx beat)
                          (do (>! out (vec arr))
                              (recur (make-array Object n) 0 (timeout beat-rate))))))
                (do (when (> idx 0)
                      (let [narray (make-array Object idx)]
                        (System/arraycopy arr 0 narray 0 idx)
                        (>! out (vec narray))))
                    (close! out)))))))
    out))


(defn distinct-by
  [by coll]
  (let [step (fn step [xs seen]
               (when-let [s (seq xs)]
                 (let [f (first s)]
                   (if (seen (by f))
                     (recur (rest s) seen)
                     (cons f (lazy-seq
                              (step (rest s) (conj seen (by f)))))))))]
    (step coll #{})))
