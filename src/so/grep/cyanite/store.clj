(ns so.grep.cyanite.store
  (:require [so.grep.cyanite.config :as config]
            [clojure.string         :as str]
            [qbits.alia             :as alia]
            [clojure.tools.logging  :refer [error info]]
            [lamina.core            :refer [channel receive-all]]))

(def path-db
  (atom #{}))

(defn insertq
  [session]
  (alia/prepare
   session
   (str
    "UPDATE metric USING TTL ? SET data = data + ? "
    "WHERE period = ? AND rollup = ? AND path = ? AND time = ?;")))

(defn fetchq
  [session]
  (alia/prepare
   session
   (str
    "SELECT path,data,time FROM metric WHERE "
    "path IN ? AND rollup = ? AND period = ? "
    "AND time >= ? AND time <= ? ORDER BY time ASC;")))

(defn prepare-path-elem-re
  [e]
  (re-pattern (format "^%s$" (str/replace e "*" ".*"))))

(defn prepare-path-query
  [q]
  (map prepare-path-elem-re (str/split q #"\.")))

(defn path-elem-matches
  [elem-re path-elem]
  (re-matches elem-re path-elem))

(defn path-matches
  [partial? query path]
  (let [path-elems (str/split path #"\.")]
    (and (or partial? (= (count query) (count path-elems)))
         (every? seq (map re-matches query path-elems)))))

(defn find-paths
  [partial? query]
  (let [depth    (count (str/split query #"\."))
        truncate (if partial?
                   (fn [e] (str/join "." (take depth (str/split e #"\."))))
                   identity)]
    (->> @path-db
         (filter (partial path-matches partial? (prepare-path-query query)))
         (map truncate)
         (set)
         (sort))))

(defn update-path-db-every
  [session interval]
  (while true
    (->> (alia/execute session "SELECT path from metric;")
         (map :path)
         (set)
         (reset! path-db)))
  (Thread/sleep (* interval 1000)))

(defn cassandra-metric-store
  [{:keys [keyspace cluster hints]
    :or   {hints {:replication {:class "SimpleStrategy"
                                :replication_factor 1}}}}]
  (info "connecting to cassandra cluster")
  (let [session (-> (alia/cluster cluster) (alia/connect))]
    (try (alia/execute session (format "USE %s;" (name keyspace)))
         (future (update-path-db-every session 60))
         session
         (catch Exception e
           (error e "could not connect to cassandra cluster")))))

(defn channel-for
  [session]
  (let [ch    (channel)
        query (insertq session)]
    (receive-all
     ch
     (fn [payload]
       (doseq [{:keys [metric path time rollup period ttl]} payload]
         (alia/execute
          session query
          :values [(int ttl) [metric] (int period) (int rollup) path time]))))
    ch))

(defn fetch
  [session paths rollup period from to]
  (let [q (alia/prepare session fetchq)]
    (alia/execute
     session q
     :values [paths (int rollup) (int period) from to])))
