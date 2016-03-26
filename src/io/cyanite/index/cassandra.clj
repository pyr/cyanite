(ns io.cyanite.index.cassandra
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]
            [globber.glob               :refer [glob]]
            [clojure.tools.logging      :refer [error]]))

(defn mk-insert-segmentq
  [session]
  (alia/prepare
   session
   "INSERT INTO segment (pos,segment) VALUES (?,?);"))

(defn mk-insert-pathq
  [session]
  (alia/prepare
   session
   "INSERT INTO path (segment, path, length) VALUES ((?,?), ?, ?);"))

(defn mk-fetch-segmentq
  [session]
  (alia/prepare
   session
   "SELECT segment from segment WHERE pos=?;"))

(defn mk-fetch-pathq
  [session]
  (alia/prepare
   session
   "SELECT path,length from path WHERE segment=(?,?);"))

(defn runq!
  [session prepared-statement values opts]
  (let [bound (alia/bind prepared-statement values)]
    (alia/execute session bound opts)))

(defrecord CassandraIndex [options session
                           insert-segmentq fetch-segmentq
                           insert-pathq fetch-pathq
                           wrcty rdcty]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)]
      (-> this
          (assoc :session session)
          (assoc :insert-segmentq (mk-insert-segmentq session))
          (assoc :insert-pathq (mk-insert-pathq session))
          (assoc :fetch-segmentq (mk-fetch-segmentq session))
          (assoc :fetch-pathq (mk-fetch-pathq session)))))
  (stop [this]
    (-> this
        (assoc :session nil)
        (assoc :insert-segmentq nil)
        (assoc :insert-pathq nil)
        (assoc :fetch-segmentq nil)
        (assoc :fetch-pathq nil)))
  index/MetricIndex
  (push-segment! [this pos segment path length]
    (runq! session insert-segmentq
           [(int pos) segment]
           {:consistency wrcty})
    (runq! session insert-pathq
           [(int pos) segment path (int length)]
           {:consistency wrcty}))
  (by-pos [this pos]
    (map :segment (runq! session fetch-segmentq
                         [(int pos)]
                         {:consistency rdcty})))
  (by-segment [this pos segment]
    (map (juxt :path :length)
         (runq! session fetch-pathq
                [(int pos) segment]
                {:consistency rdcty}))))

(defmethod index/build-index :cassandra
  [options]
  (map->CassandraIndex (dissoc options :type)))


(comment

  (let [i  (component/start (map->CassandraIndex {:options {:cluster "127.0.0.1"}} ))]
    (index/register! i "foo.bar.baz.bim")
    (index/register! i "foo.bar.baz.bim.bam.boum.barf")
    (index/register! i "foo.bar.baz.bim.bam.boum")
    (index/register! i "foo.bar.qux")
    (index/register! i "bar.bar.qux")
    (index/register! i "foo.baz.qux")
    (index/matches i "foo.bar.*" false)))
