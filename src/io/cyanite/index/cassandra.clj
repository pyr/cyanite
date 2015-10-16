(ns io.cyanite.index.cassandra
  (:require [com.stuartsierra.component :as component]
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

(defn session!
  [{:keys [cluster username password] :as opts}]
  (try
    (let [hints   (or (:hints opts)
                      {:replication
                       {:class              "SimpleStrategy"
                        :replication_factor (or (:replication_factor opts)
                                                (:replication-factor opts)
                                                (:repfactor opts)
                                                1)}})
          cluster (if (sequential? cluster) cluster [cluster])
          session (-> {:contact-points cluster}
                      (cond-> (and username password)
                        (assoc :credentials {:username username
                                             :password password}))
                      (alia/cluster)
                      (alia/connect (or (:keyspace opts) "metric")))
          rdcty   (keyword
                   (or (:read-consistency opts)
                       (:read_consistency opts)
                       (:rdcty opts)
                       :one))
          wrcty   (keyword
                   (or (:write-consistency opts)
                       (:write_consistency opts)
                       (:wrcty opts)
                       :any))]
      [session rdcty wrcty])
    (catch com.datastax.driver.core.exceptions.InvalidQueryException e
      (error e "Could not connect to cassandra. Exiting")
      (System/exit 1))))

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
    (let [[session rdcty wrcty] (session! options)]
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
