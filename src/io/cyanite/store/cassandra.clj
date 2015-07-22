(ns io.cyanite.store.cassandra
  (:require [qbits.alia       :as alia]))

(defn insertq-v2
  [session table]
  (alia/prepare
   session
   (str "UPDATE " table " USING TTL ? SET point=? WHERE id=? AND time=?;")))

(defn fetchq-v2
  [session table]
  (alia/prepare
   session
   (str "SELECT id,time,point FROM " table " WHERE "
        "id in ? AND time >= ? AND TIME <= ?;")))

(defn session!
  [{:keys [cluster username password] :as opts}]
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
    [session rdcty wrcty]))

(defn ->id
  [id-type {:keys [path resolution]}]
  (doto (.newValue id-type)
    (.setString "path" path)
    (.setString "resolution" resolution)))

(defn ->point
  [point-type {:keys [mean min max sum]}]
  (doto (.newValue point-type)
    (.setDouble "min" min)
    (.setDouble "mean" mean)
    (.setDouble "max" max)
    (.setDouble "sum" sum)))

(defn runq!
  [session prepared-statement values opts]
  (let [bound (alia/bind prepared-statement values)]
    (alia/execute session bound opts)))
