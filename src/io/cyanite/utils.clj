(ns io.cyanite.utils
  "These didn't fit anywhere else"
  (:import org.cliffc.high_scale_lib.NonBlockingHashMap))

(defprotocol MutableMap
  "Mutable map functionality"
  (entries [this] "Return a set of entries")
  (keyset [this] "Return the keyset")
  (remove! [this k] "Atomically remove and return an element")
  (contains-key [this key] "Checks if map contains the key")
  (assoc-if-absent! [this k v] "CAS type put"))

(defn nbhm
  "Yield a NonBlockingHashMap"
  []
  (let [db (NonBlockingHashMap.)]
    (reify
      clojure.lang.ITransientMap
      (assoc [this k v]
        (.put db k v)
        this)
      MutableMap
      (entries [this]
        (.entrySet db))
      (keyset [this]
        (.keySet db))
      (contains-key [this key]
        (.containsKey db key))
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
        (or (.get db k) def))
      java.lang.Object
      (toString [this]
        (.toString db)))))
