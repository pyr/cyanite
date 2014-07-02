(ns org.spootnik.cyanite.es_path
  "Implements a path store which tracks metric names backed by elasticsearch"
  (:require [clojure.tools.logging :refer [error info debug]]
            [clojure.string        :refer [split] :as str]
            [org.spootnik.cyanite.path :refer [Pathstore]]
            [clojurewerkz.elastisch.native :as esn]
            [clojurewerkz.elastisch.native.index :as esni]
            [clojurewerkz.elastisch.native.document :as esnd]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esri]
            [clojurewerkz.elastisch.rest.document :as esrd]))

(def ES_DEF_TYPE "path")
(def ES_TYPE_MAP {ES_DEF_TYPE {:properties {:tenant {:type "string" :index "not_analyzed"}
                                        :path {:type "string" :index "not_analyzed"}}}})
(defn path-depth
  "Get the depth of a path, with depth + 1 if it ends in a period"
  [path]
  (let [length (count (split path #"\."))]
    (if (= (last path) \.)
      (+ length 1)
      length)))

(defn build-doc
  "Generate a associate array of form {path: 'path', leaf: true} from the path"
  [orig-path tenant path]
  (let [depth (path-depth path)]
    (if (= orig-path path) {:path path :tenant tenant :leaf true :depth depth} {:path path :tenant tenant :leaf false :depth depth})))


(defn concat-path
  [arr new-path]
  (let [old-path (last arr)
        doc-path (if (nil? old-path) new-path (str old-path "." new-path))]
    (conj arr doc-path)))

(defn es-all-paths
  "Generate a collection of docs of {path: 'path', leaf: true} documents
  suitable for writing to elastic search"
  [path tenant]
  (let [parts (split path #"\.")
        paths (reduce concat-path (vector) parts)]
    (map (partial build-doc path tenant) paths)))


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

(defn path-exists-cache?
  [path-exists? store path]
  (if (contains? @store path)
    true
    (if (path-exists? path)
      (do (swap! store assoc path true) true)
      false)))

(defn es-rest
  [{:keys [index url]
    :or {index "cyanite_paths" url "http://localhost:9200"}}]
  (let [store (atom {})
        conn (esr/connect url)
        existsfn (partial path-exists-cache? (partial esrd/present? conn index ES_DEF_TYPE) store)
        updatefn (partial esrd/put conn index ES_DEF_TYPE)
        scrollfn (partial esrd/scroll-seq conn)
        queryfn (partial esrd/search conn index ES_DEF_TYPE)]
    (if (not (esri/exists? conn index))
      (esri/create conn index :mappings ES_TYPE_MAP))
    (reify Pathstore
      (register [this tenant path]
                (add-path updatefn existsfn tenant path))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))


(defn es-native
  [{:keys [index host port cluster_name]
    :or {index "cyanite_paths" host "localhost" port 9300}}]
  (let [store (atom {})
        conn (esn/connect [[host port]]
                         {"cluster.name" cluster_name})
        existsfn (partial path-exists-cache? (partial esnd/present? conn index ES_DEF_TYPE) store)
        updatefn (partial esnd/put conn index ES_DEF_TYPE)
        scrollfn (partial esnd/scroll-seq conn)
        queryfn (partial esnd/search conn index ES_DEF_TYPE)]
    (if (not (esni/exists? conn index))
      (esni/create conn index :mappings ES_TYPE_MAP))
    (reify Pathstore
      (register [this tenant path]
                (add-path updatefn existsfn tenant path))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))

