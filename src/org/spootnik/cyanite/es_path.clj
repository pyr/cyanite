(ns org.spootnik.cyanite.es_path
  "Implements a path store which tracks metric names backed by elasticsearch"
  (:require [clojure.tools.logging :refer [error info debug]]
            [clojure.string        :refer [split] :as str]
            [org.spootnik.cyanite.path :refer [Pathstore]]
            [org.spootnik.cyanite.util :refer [partition-or-time distinct-by go-forever go-catch]]
            [org.spootnik.cyanite.es-client :as internal-client]
            [clojurewerkz.elastisch.native :as esn]
            [clojurewerkz.elastisch.native.index :as esni]
            [clojurewerkz.elastisch.native.document :as esnd]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esri]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest.bulk :as esrb]
            [clojure.core.async :as async :refer [<! >! go chan]]))

(def ES_DEF_TYPE "path")
(def ES_TYPE_MAP {ES_DEF_TYPE {:properties {:tenant {:type "string" :index "not_analyzed"}
                                        :path {:type "string" :index "not_analyzed"}}}})
;cache disabled, see impact of batching
(def ^:const store-to-depth 2)
(def sub-path-cache (atom #{}))
(def ^:const period 46)

(defn path-depth
  "Get the depth of a path, with depth + 1 if it ends in a period"
  [path]
  (loop [cnt 1
         from-dex 0]
    (let [dex (.indexOf path period from-dex)]
      (if (= dex -1)
        cnt
        (recur (inc cnt) (inc dex))))))

(defn element
  [path depth leaf tenant]
  {:path path :depth depth :tenant tenant :leaf leaf})

(defn es-all-paths
  "Generate a collection of docs of {path: 'path', leaf: true} documents
  suitable for writing to elastic search"
  ([^String path tenant]
     (let [cache @sub-path-cache]
       (loop [acc []
              depth 1
              from-dex 0]
         (let [nxt-dex (.indexOf path period from-dex)
               leaf (= -1 nxt-dex)]
           (if leaf
             (cons (element path depth leaf tenant)
                   acc)
             (let [sub-path (.substring path 0 nxt-dex)
                   drop (and (>= store-to-depth depth)
                             (cache sub-path))]
               (recur
                (if drop
                  acc
                  (cons
                   (element sub-path depth leaf tenant)
                   acc))
                (inc depth)
                (inc nxt-dex)))))))))

(defn build-es-filter
  "generate the filter portion of an es query"
  [path tenant leafs-only]
  (let [depth (path-depth path)
        p (str/replace (str/replace path "." "\\.") "*" ".*")
        f (vector
           {:range {:depth {:from depth :to depth}}}
           {:term {:tenant tenant}}
           {:regexp {:path p :_cache true}})]
    (if leafs-only (conj f {:term {:leaf true}}) f)))

(defn build-es-query
  "generate an ES query to return the proper result set"
  [path tenant leafs-only]
  {:filtered {:filter {:bool {:must (build-es-filter path tenant leafs-only)}}}})

(defn search
  "search for a path"
  [query scroll tenant path leafs-only]
  (let [res (query :query (build-es-query path tenant leafs-only)
                   :size 100
                   :search_type "query_then_fetch"
                   :scroll "1m")
        hits (scroll res)]
    (map #(:_source %) hits)))


(defn add-path
  "write a path into elasticsearch if it doesn't exist"
  [write-key path-exists? tenant path]
  (let [paths (es-all-paths path tenant)]
    (dorun (map #(if (not (path-exists? (:path %)))
                   (write-key (:path %) %)) paths))))

(defn dont-exist
  [conn index type]
  (fn [paths dos-and-donts-processor]
    (when-let [ids (seq (map #(hash-map :_id (or (:path %) %)) paths))]
      (internal-client/multi-get
       conn index type ids
       (fn [resp]
         (let [found (set (map :_id (remove nil? resp)))]
           (dos-and-donts-processor
            (reduce (fn [[exist dont] p]
                      (if (found (or (:path p) p))
                        [(cons p exist) dont]
                        [exist (cons p dont)]))
                    [[] []]
                    paths))))))))

(defn bulk-update
  [conn index type]
  (fn [paths]
    (internal-client/multi-update
     conn index type paths
     #(debug "Failed bulk update, full response: " %))))

(defn es-rest
  [{:keys [index url]
    :or {index "cyanite_paths" url "http://localhost:9200"}}]
  (let [full-path-cache (atom #{})
        conn (esr/connect url)
        dontexistsfn (dont-exist conn index ES_DEF_TYPE)
        bulkupdatefn (bulk-update conn index ES_DEF_TYPE)
        existsfn (partial esrd/present? conn index ES_DEF_TYPE)
        updatefn (partial esrd/put conn index ES_DEF_TYPE)
        scrollfn (partial esrd/scroll-seq conn)
        queryfn (partial esrd/search conn index ES_DEF_TYPE)]
    (if (not (esri/exists? conn index))
      (esri/create conn index :mappings ES_TYPE_MAP))
    (reify Pathstore
      (register [this tenant path]
                (add-path updatefn existsfn tenant path))
      (channel-for [this]
        (let [es-chan (chan 10000)
              es-chan-p (partition-or-time 1000 es-chan 1000 10)
              checked-paths (chan 10000)
              checked-paths-p (partition-or-time 1000 checked-paths 1000 10)
              all-paths (chan 10000)
              all-paths-p (partition-or-time 1000 all-paths 1000 5)
              create-path (chan 10000)
              create-path-p (partition-or-time 100 create-path 1000 5)]
          (go-forever
           (let [ps (<! es-chan-p)
                 cache @full-path-cache]
             (dontexistsfn
              (remove cache ps)
              (fn [[exist dont]]
                (go-catch
                  (debug "Full Paths Fnd " (count exist) ", creating " (count dont))
                  (doseq [p dont]
                    (>! checked-paths p))
                  (when (seq exist)
                    (swap! full-path-cache
                           clojure.set/union
                           (set exist))))))))
          (go-forever
           (let [ps (<! checked-paths-p)]
             (go-catch
               (doseq [p ps]
                 (doseq [ap (distinct-by :path (es-all-paths p ""))]
                   (>! all-paths ap))))))
          (go-forever
           (let [ps (<! all-paths-p)]
             (dontexistsfn
              ps
              (fn [[exist dont]]
                (go-catch
                 (debug "Fnd " (count exist) ", creating " (count dont))
                 (doseq [p dont]
                   (>! create-path p))
                 (when (seq exist)
                   (let [cached-sub-paths @sub-path-cache
                         sub-paths-to-store ((comp set (partial map :path) filter)
                                             #(and (>= store-to-depth (:depth %))
                                                   (not (cached-sub-paths (:path %))))
                                             exist)]
                     (swap! sub-path-cache clojure.set/union sub-paths-to-store))))))))
          (go-forever
           (let [ps (<! create-path-p)]
             (bulkupdatefn ps)))
          es-chan))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))

(defn es-native
  [{:keys [index host port cluster_name]
    :or {index "cyanite" host "localhost" port 9300 cluster_name "elasticsearch"}}]
  (let [conn (esn/connect [[host port]]
                         {"cluster.name" cluster_name})
        existsfn (partial esnd/present? conn index ES_DEF_TYPE)
        updatefn (partial esnd/async-put conn index ES_DEF_TYPE)
        scrollfn (partial esnd/scroll-seq conn)
        queryfn (partial esnd/search conn index ES_DEF_TYPE)]
    (if (not (esni/exists? conn index))
      (esni/create conn index :mappings ES_TYPE_MAP))
    (reify Pathstore
      (register [this tenant path]
        (add-path updatefn existsfn tenant path))
      (channel-for [this]
        (let [es-chan (chan 1000)
              all-paths (chan 1000)
              create-path (chan 1000)]
          (go-forever
            (let [p (<! es-chan)]
              (doseq [ap (es-all-paths p "")]
                (>! all-paths ap))))
          (go-forever
            (let [p (<! all-paths)]
              (when-not (existsfn (:path p))
                (>! create-path p))))
          (go-forever
            (let [p (<! create-path)]
              (updatefn (:path p) p)))
          es-chan))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))
