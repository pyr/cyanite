(ns io.cyanite.index.cassandra
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]
            [globber.glob               :refer [glob]]
            [clojure.tools.logging      :refer [error]]))

(defn mk-insert-pathq
  [session]
  (alia/prepare
   session
   "INSERT INTO path (prefix, path, length) VALUES (?, ?, ?);"))

(defn mk-fetch-pathq
  [session]
  (alia/prepare
   session
   "SELECT path,length from path WHERE path=?;"))

(defn mk-fetch-prefixes
  [session]
  (alia/prepare
   session
   "SELECT * from path WHERE path LIKE ? AND length <= ? ALLOW FILTERING;"))

(defn mk-fetch-leaves
  [session]
  (alia/prepare
   session
   "SELECT * from path WHERE path LIKE ? AND length = ? ALLOW FILTERING;"))

(defn runq!
  [session prepared-statement values opts]
  (let [bound (alia/bind prepared-statement values)]
    (alia/execute session bound opts)))

(defn index-of-first
  [chars s]
  (reduce
   (fn [idx char]
     (let [c (or (clojure.string/index-of s char) -1)]
       (if (and (>= c 0)
                (> c idx))
         c
         idx)))
   -1
   chars)
  )
(defn glob-to-like
  [pattern]
  (if-let [pos (index-of-first [\* \. \? \[] pattern)]
    (str (subs pattern 0 pos) "%")
    pattern))

(defrecord CassandraIndex [options session
                           fetch-leaves
                           insert-pathq fetch-pathq
                           wrcty rdcty]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)]
      (-> this
          (assoc :session session)
          (assoc :insert-pathq (mk-insert-pathq session))
          (assoc :fetch-pathq (mk-fetch-pathq session))
          (assoc :fetch-prefixes nil)
          (assoc :fetch-leaves nil))))
  (stop [this]
    (-> this
        (assoc :session nil)
        (assoc :insert-pathq nil)
        (assoc :fetch-pathq nil)
        (assoc :fetch-prefixes nil)
        (assoc :fetch-leaves nil)))
  index/MetricIndex
  (push-segment! [this pos segment path length]
    (println path)
    (runq! session insert-pathq
           ;; TODO possibly butlast?
           [(first (clojure.string/split path #"\.")) path (int length)]
           {:consistency wrcty}))
  (prefixes [this pattern]
    (println (str "SELECT * from path WHERE path LIKE '"
                  (glob-to-like pattern)
                  "' AND length = "
                  (count (clojure.string/split pattern #"\."))
                  " ALLOW FILTERING;"))
    (let [res (alia/execute session
                            (str "SELECT * from path WHERE path LIKE '"
                                 (glob-to-like pattern)
                                 "' AND length = "
                                 (count (clojure.string/split pattern #"\."))
                                 " ALLOW FILTERING;")
                            {:consistency wrcty})]
      res
      )
    )
  (leaves [this pattern]
    (let [res (alia/execute session
                            (str "SELECT * from path WHERE path LIKE '"
                                 (glob-to-like pattern)
                                 "' AND length <= "
                                 (count (clojure.string/split pattern #"\."))
                                 " ALLOW FILTERING;")
                            {:consistency wrcty})]
      res
      )
    ))

(defmethod index/build-index :cassandra
  [options]
  (map->CassandraIndex {:options (dissoc options :type)}))


(comment

  (let [i (component/start (map->CassandraIndex {:options {:cluster "127.0.0.1"}} ))]
    (index/register! i"foo.bar.baz.bim")
    (index/register! i"foo.bar.baz.bim.bam.boum.barf")
    (index/register! i"foo.bar.baz.bim.bam.boum")
    (index/register! i"foo.bar.qux")
    (index/register! i"bar.bar.qux")
    (index/register! i"foo.baz.qux")
    (index/prefixes i"foo.bar.*"))

  (let [i  (component/start (index/map->AtomIndex  {} ))]
    (index/register! i"foo.bar.baz.bim")
    (index/register! i"foo.bar.baz.bim.bam.boum.barf")
    (index/register! i"foo.bar.baz.bim.bam.boum")
    (index/register! i"foo.bar.qux")
    (index/register! i"bar.bar.qux")
    (index/register! i"foo.baz.qux")
    (index/matches i"foo.bar.*" false)))
