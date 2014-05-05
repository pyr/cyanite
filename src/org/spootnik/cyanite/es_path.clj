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

(defn path-depth
  "Get the depth of a path, with depth + 1 if it ends in a period"
  [path]
  (let [length (count (split path #"\."))]
    (if (= (last path) \.)
      (+ length 1)
      length)))

(defn build-doc
  "Generate a associate array of form {path: 'path', leaf: true} from the path"
  [orig-path path]
  (let [depth (path-depth path)]
    (if (= orig-path path) {:path path :leaf true :depth depth} {:path path :leaf false :depth depth})))


(defn concat-path
  [arr new-path]
  (let [old-path (last arr)
        doc-path (if (nil? old-path) new-path (str old-path "." new-path))]
    (conj arr doc-path)))

(defn es-all-paths
  "Generate a collection of docs of {path: 'path', leaf: true} documents
  suitable for writing to elastic search"
  [path]
  (let [parts (split path #"\.")
        paths (reduce concat-path (vector) parts)]
    (map (partial build-doc path) paths)))


(defn build-es-filter
  "generate the filter portion of an es query"
  [path leafs-only]
  (let [depth (path-depth path)
        f (vector {:range {:depth {:from depth :to depth}}})]
    (if leafs-only (conj f {:term {:leaf true}}) f)))


(defn build-es-query
  "generate an ES query to return the proper result set"
  [path leafs-only]
  { :filtered {
               :query {:bool {:must {:wildcard {:path path}}}}
               :filter {:bool {:must (build-es-filter path leafs-only)}}}})

(defn search
  "search for a path"
  [query scroll tenant path leafs-only]
  (let [res (query (str tenant "path") :query (build-es-query path leafs-only) :size 100 :search_type "query_then_fetch" :scroll "1m")
        hits (scroll res)]
    (map #(:_source %) hits)))


(defn add-path
  "write a path into elasticsearch if it doesn't exist"
  [write-key path-exists? tenant path]
  (let [paths (es-all-paths path)]
    (dorun (map #(if (not (path-exists? (str tenant "path") (:path %)))
                   (write-key (str tenant "path") (:path %) %)) paths))))

(defn es-rest
  [{:keys [index url]
    :or {index "cyanite_paths" url "http://localhost:9200"}}]
  (let [conn (esr/connect url)
        existsfn (partial esrd/present? conn index)
        updatefn (partial esrd/put conn index)
        scrollfn (partial esrd/scroll-seq conn)
        queryfn (partial esrd/search conn index)]
    (if (not (esri/exists? conn "cyanite_paths"))
      (esri/create conn "cyanite_paths" :mappings {"path" {:properties {:path {:type "string" :index "not_analyzed"}}}}))
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
  (let [conn (esn/connect [[host port]]
                         {"cluster.name" cluster_name})
        existsfn (partial esnd/present? conn index)
        updatefn (partial esnd/put conn index)
        scrollfn (partial esnd/scroll-seq conn)
        queryfn (partial esnd/search conn index)]
    (if (not (esni/exists? conn "cyanite_paths"))
      (esni/create conn "cyanite_paths" :mappings {"path" {:properties {:path {:type "string" :index "not_analyzed"}}}}))
    (reify Pathstore
      (register [this tenant path]
                (add-path updatefn existsfn tenant path))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))

