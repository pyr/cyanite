(ns io.cyanite.store.cassandra
  (:require [qbits.alia            :as alia]
            [qbits.alia.codec      :as codec]
            [io.cyanite.engine.rule :as rule]
            [clojure.tools.logging :refer [error]]))

(defn udt-value->map
  [udt-value]
  (let [udt-type (.getType udt-value)
        udt-type-iter (.iterator udt-type)
        len (.size udt-type)]
    (loop [udt (transient {})
           idx' 0]
      (if (= idx' len)
        (persistent! udt)
        (let [^com.datastax.driver.core.UserType$Field type (.next udt-type-iter)]
          (recur (assoc! udt
                         (-> type .getName keyword)
                         (codec/deserialize udt-value idx'))
                 (unchecked-inc-int idx')))))))

(extend-protocol codec/PCodec
  com.datastax.driver.core.UDTValue
  (encode [x] x)
  (decode [udt-value]
    (let [m (udt-value->map udt-value)]
      (if (= "metric_resolution" (.getTypeName (.getType udt-value)))
        (rule/map->Resolution m)
        m))))

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
        "id IN ? AND time >= ? AND TIME <= ?;")))

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
                        (assoc :credentials {:user username
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

(defn runq-async!
  [session prepared-statement values opts]
  (let [bound (alia/bind prepared-statement values)]
    (alia/execute-async session bound opts)))

(defn get-types
  [session]
  (let [->point (alia/udt-encoder session "metric_point")
        ->id    (alia/udt-encoder session "metric_id")
        ->res   (alia/udt-encoder session "metric_resolution")]
    [(fn [{:keys [path resolution]}]
       (let [precision (-> resolution :precision int)
             period    (-> resolution :period int)]
         (->id {:path path
                :resolution (->res {:precision precision
                                    :period    period})})))
     (fn [{:keys [mean min max sum]}]
       (->point {:mean (double mean)
                 :min  (double min)
                 :max  (double max)
                 :sum  (double sum)}))]))
