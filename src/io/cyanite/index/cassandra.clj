(ns io.cyanite.index.cassandra
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.index           :as index]
            [io.cyanite.utils           :refer [contains-key]]
            [qbits.alia                 :as alia]
            [globber.glob               :refer [glob]]
            [clojure.string             :refer [index-of join split]]
            [clojure.tools.logging      :refer [error]]))

(defn mk-insert-pathq
  [session]
  (alia/prepare
   session
   "INSERT INTO path (prefix, path, length) VALUES (?, ?, ?);"))

(defn mk-insert-segmentq
  [session]
  (alia/prepare
   session
   "INSERT INTO segment (pos, segment, length, leaf) VALUES (?, ?, ?, ?);"))

(defn runq!
  [session prepared-statement values opts]
  (let [bound (alia/bind prepared-statement values)]
    (alia/execute session bound opts)))

(defn index-of-first
  [chars s]
  (reduce
   (fn [idx char]
     (let [c (or (index-of s char) -1)]
       (if (and (>= c 0)
                (> c idx))
         c
         idx)))
   -1
   chars))

(defn glob-to-like
  [pattern]
  (if-let [pos (index-of-first [\* \. \? \[] pattern)]
    (str (subs pattern 0 pos) "%")
    pattern))

(defn compose-parts
  [path]
  (let [parts (split path #"\." )]
    (map
     #(vector % (clojure.string/join "." (take % parts)))
     (range 1 (inc (count parts))))))

(defn prefix-info
  [pattern-length [path length leaf]]
  {:text          (last (split path #"\."))
   :id            path
   :allowChildren (not leaf)
   :expandable    (not leaf)
   :leaf          leaf})

(defrecord CassandraIndex [options session
                           insert-segmentq insert-pathq
                           wrcty rdcty]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)]
      (-> this
          (assoc :session session)
          (assoc :insert-pathq (mk-insert-pathq session))
          (assoc :insert-segmentq (mk-insert-segmentq session)))))
  (stop [this]
    (-> this
        (assoc :session nil)
        (assoc :insert-pathq nil)
        (assoc :insert-segmentq nil)))
  index/MetricIndex
  (register! [this path]
    (let [parts  (compose-parts path)
          length (count parts)]
      (doseq [[i part] parts]
        (runq! session insert-segmentq
               [(int i)
                part
                length
                (= length i)]
               {:consistency wrcty}))
      (runq! session insert-pathq
             [(->> (split path #"\.")
                   (butlast)
                   (join "."))
              path
              (int (count parts))]
             {:consistency wrcty})))
  (prefixes [this pattern]
    (let [pos      (count (split pattern #"\."))
          res      (alia/execute session
                                 (str "SELECT * from segment where "
                                      (if (> pos 1)
                                        (str "segment LIKE '"
                                             (glob-to-like pattern)
                                             "' AND ")
                                        "")
                                      "pos = " pos)
                                 {:consistency wrcty})
          filtered (set (glob pattern (map :segment res)))]
      (->> res
           (filter (fn [{:keys [segment]}]
                     (not (nil? (get filtered segment)))))
           (map (juxt :segment :length :leaf))
           (map (partial prefix-info pos)))))
  (leaves [this pattern]
    (let [res      (alia/execute session
                                 (str "SELECT * from path WHERE path LIKE '"
                                      (glob-to-like pattern)
                                      "'")
                                 {:consistency wrcty})
          filtered (set (glob pattern (map :path res)))]
      (filter (fn [{:keys [path]}]
                (not (nil? (get filtered path)))) res))))

(defmethod index/build-index :cassandra
  [options]
  (map->CassandraIndex {:options (dissoc options :type)}))
