(ns io.cyanite.utils
  (:import org.cliffc.high_scale_lib.NonBlockingHashMap))

(defprotocol MutableMap
  (keyset [this])
  (remove! [this k])
  (assoc-if-absent! [this k v]))

(defn nbhm
  []
  (let [db (NonBlockingHashMap.)]
    (reify
      clojure.lang.ITransientMap
      (assoc [this k v]
        (.put db k v)
        this)
      MutableMap
      (keyset [this]
        (.keySet db))
      (remove! [this k]
        (.remove db k))
      (assoc-if-absent! [this k v]
        (.putIfAbsent db k v))
      clojure.lang.Seqable
      (seq [this]
        (let [keys (.keySet db)]
          (map #(.get db %) (.keySet db))))
      clojure.lang.ILookup
      (valAt [this k]
        (.get db k))
      (valAt [this k def]
        (or (.get db k) def)))))

(defn now!
  []
  (quot (System/currentTimeMillis) 1000))
