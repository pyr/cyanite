(ns org.spootnik.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [clojure.string              :as str]
            [qbits.alia                  :as alia]
            [org.spootnik.cyanite.util :refer [partition-or-time go-forever go-catch]]
            [clojure.tools.logging       :refer [error info debug]]
            [lamina.core                 :refer [channel receive-all]]
            [clojure.core.async :as async :refer [<! >! go chan]])
  (:import [com.datastax.driver.core
            BatchStatement
            PreparedStatement]))

(set! *warn-on-reflection* true)

(defprotocol Metricstore
  (insert [this ttl data tenant rollup period path time])
  (channel-for [this])
  (fetch [this agg paths tenant rollup period from to]))

;;
;; The following contains necessary cassandra queries. Since
;; cyanite relies on very few queries, I decided against using
;; hayt

(defn insertq
  "Yields a cassandra prepared statement of 6 arguments:

* `ttl`: how long to keep the point around
* `metric`: the data point
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `path`: name of the metric
* `time`: timestamp of the metric, should be divisible by rollup"
  [session]
  (alia/prepare
   session
   (str
    "UPDATE metric USING TTL ? SET data = data + ? "
    "WHERE tenant = '' AND rollup = ? AND period = ? AND path = ? AND time = ?;")))

(defn fetchq
  "Yields a cassandra prepared statement of 6 arguments:

* `paths`: list of paths
* `rollup`: interval between points at this resolution
* `period`: rollup multiplier which determines the time to keep points for
* `min`: return points starting from this timestamp
* `max`: return points up to this timestamp
* `limit`: maximum number of points to return"
  [session]
  (alia/prepare
   session
   (str
    "SELECT path,data,time FROM metric WHERE "
    "path IN ? AND tenant = '' AND rollup = ? AND period = ? "
    "AND time >= ? AND time <= ? ORDER BY time ASC;")))


(defn useq
  "Yields a cassandra use statement for a keyspace"
  [keyspace]
  (format "USE %s;" (name keyspace)))

;;
;; The next section contains a series of path matching functions


(defmulti aggregate-with
  "This transforms a raw list of points according to the provided aggregation
   method. Each point is stored as a list of data points, so multiple
   methods make sense (max, min, mean). Additionally, a raw method is
   provided"
  (comp first list))

(defmethod aggregate-with :mean
  [_ {:keys [data] :as metric}]
  (if (seq data)
    (-> metric
        (dissoc :data)
        (assoc :metric (/ (reduce + 0.0 data) (count data))))
    metric))

(defmethod aggregate-with :sum
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (reduce + 0.0 data))))

(defmethod aggregate-with :max
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply max data))))

(defmethod aggregate-with :min
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric (apply min data))))

(defmethod aggregate-with :raw
  [_ {:keys [data] :as metric}]
  (-> metric
      (dissoc :data)
      (assoc :metric data)))

(defn max-points
  "Returns the maximum number of points to expect for
   a given resolution, time range and number of paths"
  [paths rollup from to]
  (-> (- to from)
      (/ rollup)
      (long)
      (inc)
      (* (count paths))
      (int)))

(defn fill-in
  "Fill in fetched data with nil metrics for a given time range"
  [nils [path data]]
  (hash-map path
            (->> (group-by :time data)
                 (merge nils)
                 (map (comp first val))
                 (sort-by :time)
                 (map :metric))))

(defn- batch
  "Creates a batch of prepared statements"
  [^PreparedStatement s values]
  (let [b (BatchStatement.)]
    (doseq [v values]
      (.add b (.bind s (into-array Object v))))
    b))

(defn cassandra-metric-store
  "Connect to cassandra and start a path fetching thread.
   The interval is fixed for now, at 1minute"
  [{:keys [keyspace cluster hints]
    :or   {hints {:replication {:class "SimpleStrategy"
                                :replication_factor 1}}}}]
  (info "creating cassandra metric store")
  (let [session (-> (alia/cluster {:contact-points [cluster]})
                    (alia/connect keyspace))
        insert! (insertq session)
        fetch!  (fetchq session)]
    (reify
      Metricstore
      (channel-for [this]
        (let [ch (chan 10000)
              ch-p (partition-or-time 500 ch 500 5)]
          (go-forever
           (let [payload (<! ch-p)]
             (try
               (let [values (map
                             #(let [{:keys [metric path time rollup period ttl]} %]
                                [(int ttl) [metric] (int rollup) (int period) path time])
                             payload)]
                 (alia/execute-async
                  session
                  (batch insert! values)
                  {:consistency :any
                   :success (fn [_] (debug "written batch"))
                   :error (fn [e] (info "Casandra error: " e))}))
               (catch Exception e
                 (info e "Store processing exception")))))
          ch))
      (insert [this ttl data tenant rollup period path time]
        (alia/execute-async
         session
         insert!
         {:values [ttl data tenant rollup period path time]}))
      (fetch [this agg paths tenant rollup period from to]
        (debug "fetching paths from store: " paths rollup period from to)
        (if-let [data (and (seq paths)
                           (->> (alia/execute
                                 session fetch!
                                 {:values [paths (int rollup) (int period)
                                           from to]
                                  :fetch-size Integer/MAX_VALUE})
                                (map (partial aggregate-with (keyword agg)))
                                (seq)))]
          (let [min-point  (:time (first data))
                max-point  (-> to (quot rollup) (* rollup))
                nil-points (->> (range min-point (inc max-point) rollup)
                                (map (fn [time] {time [{:time time}]}))
                                (reduce merge {}))
                by-path    (->> (group-by :path data)
                                (map (partial fill-in nil-points))
                                (reduce merge {}))]
            {:from min-point
             :to   max-point
             :step rollup
             :series by-path})
          {:from from
           :to to
           :step rollup
           :series {}})))))
