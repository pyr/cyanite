(ns org.spootnik.cyanite.store
  "Implements a metric store on top of cassandra. This currently
   relies on a single schema. All cassandra interaction bits
   should quickly be abstracted with a protocol to more easily
   swap implementations"
  (:require [org.spootnik.cyanite.config :as config]
            [clojure.string              :as str]
            [qbits.alia                  :as alia]
            [clojure.tools.logging       :refer [error info debug]]
            [lamina.core                 :refer [channel receive-all]]))

(def
  ^{:doc "Store an in-memory set of all known metric paths"}
  path-db
  (atom #{}))

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
    "AND time >= ? AND time <= ? ORDER BY time ASC LIMIT ?;")))


(def
  ^{:doc "This query returns only paths, to get a list of
metrics. Right now it is way too expensive and should be computed differently"}
  pathq "SELECT distinct tenant, path, rollup, period from metric;")

(defn useq
  "Yields a cassandra use statement for a keyspace"
  [keyspace]
  (format "USE %s;" (name keyspace)))

;;
;; The next section contains a series of path matching functions


(defn prepare-path-elem-re
  "Each graphite path elem may contain '*' wildcards, this
   functions yields a regexp pattern fro this format"
  [e]
  (re-pattern (format "^%s$" (str/replace e "*" ".*"))))

(defn prepare-path-query
  "For a complete path query, yield a list of regexp pattern
   for each element"
  [q]
  (map prepare-path-elem-re (str/split q #"\.")))

(defn path-matches?
  "Predicate testing a path against a query. partial? determines
   whether the query should be treated as a prefix query or an
   absolute query"
  [partial? query path]
  (let [path-elems (str/split path #"\.")]
    (and (or partial? (= (count query) (count path-elems)))
         (every? seq (map re-matches query path-elems)))))

(defn truncate-path
  "When doing prefix searches, we're only interested in returning
   prefixes, this function yields a map of two keys:

   * leaf: indicates whether this is a prefix or an actual point
   * path: the prefix"
  [query path]
  (let [depth     (count (str/split query #"\."))
        truncated (str/join "." (take depth (str/split path #"\.")))]
    {:leaf (= truncated path) :path truncated}))

(defn find-paths
  "Find either prefix or absolute matches for a query"
  [partial? query]
  (let [depth    (count (str/split query #"\."))]
    (filter (partial path-matches? partial? (prepare-path-query query))
            @path-db)))

(defn find-prefixes
  "Find prefix matches for a query and then format for graphite output"
  [query]
  (->> (find-paths true query)
       (map (partial truncate-path query))
       (set)
       (sort-by :path)))

(defn update-path-db-every
  "At each interval, fetch all known paths, and store the
   resulting set in path-db"
  [session interval]
  (while true
    (->> (alia/execute session pathq)
         (map :path)
         (set)
         (reset! path-db))
    (Thread/sleep (* interval 1000))))

(defn cassandra-metric-store
  "Connect to cassandra and start a path fetching thread.
   The interval is fixed for now, at 1minute"
  [{:keys [keyspace cluster hints]
    :or   {hints {:replication {:class "SimpleStrategy"
                                :replication_factor 1}}}}]
  (info "connecting to cassandra cluster")
  (let [session (-> (alia/cluster cluster) (alia/connect))]
    (try (alia/execute session (useq keyspace))
         (future (update-path-db-every session 60))
         session
         (catch Exception e
           (error e "could not connect to cassandra cluster")))))

(defn channel-for
  "Yields a lamina channel which expects lists of metrics and will
   insert each for every resolution"
  [session]
  (let [ch    (channel)
        query (insertq session)]
    (receive-all
     ch
     (fn [payload]
       (doseq [{:keys [metric path time rollup period ttl]} payload]
         (alia/execute
          session query
          :values [(int ttl) [metric] (int rollup) (int period) path time]))))
    ch))

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

(defn fetch
  "Fetch metrics for a given resolution. Once fetched, formats
   metrics in a way graphite can easily consume"
  [session agg paths rollup period from to]
  (debug "fetching paths from store: " paths rollup period from to
         (max-points paths rollup from to))

  (if paths
    (let [q          (fetchq session)
          data       (->> (alia/execute
                           session q
                           :values [paths (int rollup) (int period) from to
                                    (max-points paths rollup from to)])
                          (map (partial aggregate-with (keyword agg))))
          min-point  (:time (first data))
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
     :series {}}))
