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
            [clojurewerkz.elastisch.rest.document :as esrd]
            [clojure.core.async :as async :refer [<! >! go chan]]))

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
    (if (= orig-path path)
      {:path path :tenant tenant :leaf true :depth depth}
      {:path path :tenant tenant :leaf false :depth depth})))


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
  (let [res (query :query (build-es-query path tenant leafs-only) :size 100 :search_type "query_then_fetch" :scroll "1m")
        hits (scroll res)]
    (map #(:_source %) hits)))


(defn add-path
  "write a path into elasticsearch if it doesn't exist"
  [write-key path-exists? tenant path]
  (let [paths (es-all-paths path tenant)]
    (dorun (map #(if (not (path-exists? (:path %)))
                   (write-key (:path %) %)) paths))))

(defn es-rest
  [{:keys [index url]
    :or {index "cyanite_paths" url "http://localhost:9200"}}]
  (let [conn (esr/connect url)
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
              all-paths (chan 10000)
              create-path (chan 10000)]
          (go
            (while true
              (let [ps (<! (async/partition 1000 es-chan 10))]
                (go
                  (doseq [p ps]
                    (doseq [ap (es-all-paths p "")]
                      (>! all-paths ap)))))))
          (go
            (while true
              (let [ps (<! (async/partition 1000 all-paths))]
                (go
                  (doseq [p ps]
                      (when-not (existsfn (:path p))
                        (>! create-path p)))))))
          (go
            (while true
              (let [ps (<! (async/partition 100 create-path))]
                (doseq [p ps]
                  (go
                    (updatefn (:path p) p))))))
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
          (go
            (while true
              (let [p (<! es-chan)]
                (doseq [ap (es-all-paths p "")]
                  (>! all-paths)))))
          (go
            (while true
              (let [p (<! all-paths)]
                (when-not (existsfn (:path p))
                  (>! create-path p)))))
          (go
            (while true
              (let [p (<! create-path)]
                (updatefn (:path p) p))))
          es-chan))
      (prefixes [this tenant path]
                (search queryfn scrollfn tenant path false))
      (lookup [this tenant path]
              (map :path (search queryfn scrollfn tenant path true))))))
