(ns io.cyanite.index.cassandra
  (:import  [com.github.benmanes.caffeine.cache Caffeine CacheLoader LoadingCache]
            [java.util.concurrent TimeUnit])
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.index           :as index]
            [io.cyanite.utils           :refer [contains-key]]
            [qbits.alia                 :as alia]
            [globber.glob               :refer [glob]]
            [clojure.string             :refer [index-of join split]]
            [clojure.tools.logging      :refer [error]]))

(def default-cache-ttl-in-ms 60000)

(defn mk-insert-segmentq
  [session]
  (alia/prepare
   session
   "INSERT INTO segment (parent, segment, pos, length, leaf) VALUES (?, ?, ?, ?, ?);"))

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
  (let [pos (index-of-first [\* \? \[] pattern)]
    (cond
      (= 0 pos)  nil
      (= -1 pos) pattern
      :default   (str (subs pattern 0 pos) "%"))))

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
   :path          path
   :allowChildren (not leaf)
   :expandable    (not leaf)
   :leaf          leaf})

(defn native-sasi-index
  [session pattern parts]
  (let [globbed       (glob-to-like pattern)
        pos           (count parts)
        globbed-parts (if (nil? globbed) [] (split globbed #"\."))]
    (alia/execute
     session
     (str "SELECT * from segment WHERE "
          (cond
            ;; Top-level query, return root only
            (= pattern "*")                         "parent = 'root' AND pos = 1"
            ;; Postfix wildcard query (`*.abc` and alike), optimise by position
            (= globbed nil)                         (str "pos = " pos)
            ;; Prefix wildcard query (`abc.*` and alike), add parent
            (= (count parts) (count globbed-parts)) (str "parent = '" (join "." (butlast globbed-parts)) "'"
                                                         " AND pos = " (count parts))
            ;; Prefix wildcard query, (`abc.*.def`), can't use position
            :else                                   (str "pos = " (count parts)
                                                         " AND segment LIKE '" globbed "' ALLOW FILTERING" ))))))

(defn with-cyanite-tokenizer
  [session pattern parts]
  (let [globbed       (glob-to-like pattern)
        pos           (count parts)
        globbed-parts (if (nil? globbed) [] (split globbed #"\."))]
    (alia/execute session
                  (cond
                    ;; Top-level query, return root only
                    (= pattern "*")                         (str "SELECT * FROM segment WHERE parent = 'root' AND pos = 1")
                    ;; Postfix wildcard query (`*.abc` and alike), optimise by position
                    (= globbed nil)                         (str "SELECT * FROM segment WHERE pos = " pos)
                    ;; Prefix wildcard query (`abc.*` and alike), add parent
                    :else                                   (str "SELECT * FROM segment WHERE segment LIKE '" pattern "' AND pos = " pos " ALLOW FILTERING")))))

(defn load-prefixes-fn
  [session index-query-fn pattern]
  (let [parts    (split pattern #"\.")
        pos      (count parts)
        res      (index-query-fn session pattern parts)
        filtered (set (glob pattern (map :segment res)))]
    (->> res
         (filter (fn [{:keys [segment]}]
                   (not (nil? (get filtered segment)))))
         (map (juxt :segment :length :leaf))
         (map (partial prefix-info pos)))))

(defrecord CassandraIndex [options session ^LoadingCache cache
                           aggregates pattern
                           insert-segmentq insert-pathq index-query-fn
                           wrcty rdcty]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)
          index-query-fn        (if (:with_tokenizer options)
                                  with-cyanite-tokenizer
                                  native-sasi-index)
          aggregates            (or (:aggregates options) [])]
      (assoc this
             :aggregates      aggregates
             :pattern         (index/make-pattern aggregates)
             :session         session
             :insert-segmentq (mk-insert-segmentq session)
             :cache           (-> (Caffeine/newBuilder)
                                  (.expireAfterWrite
                                   (or (:cache_ttl_in_ms options)
                                       default-cache-ttl-in-ms)
                                   TimeUnit/MILLISECONDS)
                                  (.build (reify CacheLoader
                                            (load [this pattern]
                                              (load-prefixes-fn session index-query-fn pattern)))))
             )))
  (stop [this]
    (-> this
        (assoc :session nil)
        (assoc :insert-segmentq nil)))
  index/MetricIndex
  (register! [this path]
    (let [parts  (compose-parts path)
          fpart  (first parts)
          parts  (cons [[0 "root"] fpart] (partition 2 1 parts))
          length (count parts)]
      (doseq [[[_ parent] [i part]] parts]
        (runq! session insert-segmentq
               [parent
                part
                (int i)
                length
                (= length i)]
               {:consistency wrcty}))))
  (prefixes [this pattern]
    (.get cache pattern))

  (multiplex-aggregates [this prefixes]
    (index/multiplex-aggregates-paths aggregates prefixes))
  (extract-aggregate   [this prefix]
    (index/extract-aggregate-path pattern prefix)))

(defmethod index/build-index :cassandra
  [options]
  (map->CassandraIndex {:options (dissoc options :type)}))
