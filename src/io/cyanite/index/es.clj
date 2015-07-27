(ns io.cyanite.index.es
  (:require [qbits.jet.client.http :as http]
            [clojure.core.async    :as async]
            [cheshire.core         :as json]
            [clojure.string        :as s]
            [clojure.string        :refer [split]]))

(defprotocol JSONClient
  (request! [this method path] [this method path body]))

(defprotocol SearchClient
  (register! [this path ttl])
  (query [this q bound?]))

(defn json-client
  [url]
  (let [client (http/client)]
    (reify JSONClient
      (request! [this method path]
        (request! this method path nil))
      (request! [this method path body]
        (let [req {:url (format "%s/%s" url (name path))
                   :as :json
                   :follow-redirects? true
                   :content-type "application/json"
                   :body (when body (json/generate-string body))
                   :method method}
              resp (async/<!! (http/request client req))]
          (if (= 2 (quot (:status resp) 100))
            (update resp :body async/<!!)
            (dissoc resp :body)))))))

(defn path-length
  [path]
  (count (split path #"\.")))

(defn murmur3
  [^String path]
  (clojure.lang.Murmur3/hashUnencodedChars path))

(defn try-mapping
  [client type mapping]
  (let [url  (format "_mapping/%s" type)
        body (:body (request! client :get url))]
    (when (or (nil? body) (empty? body))
      (request! client :put url mapping))))

(def default-mapping
  {:path {:properties {:path   {:type  "string"
                                :index "not_analyzed"}
                       :length {:type  "integer"
                                :index "not_analyzed"
                                :store false}}}})

(defn ->query-filter
  [q]
  (if (neg? (.indexOf q "*"))
    {:term {:path q}}
    {:regexp {:path (s/replace q "*" ".*")}}))

(defn bound-query
  [q]
  (let [l (count (split q #"\."))]
    {:query
     {:filtered
      {:query  {:term {:length l}}
       :filter (->query-filter q)}}}))

(defn unbound-query
  [q]
  {:query
   {:filtered
    {:query  {:match_all {}}
     :filter (->query-filter q)}}})

(defn client
  [options]
  (let [url     (or (:url options) "http://localhost:9200/cyanite")
        type    (or (:es-type options) "path")
        client  (json-client url)
        mapping (or (:mapping options) default-mapping)]
    (try-mapping client type mapping)
    (reify SearchClient
      (register! [this path ttl]
        (request! client :put (format "%s/%s?ttl=%d" type (murmur3 path) ttl)
                  {:path path :length (path-length path)}))
      (query [this pattern bound?]
        (let [url (format "%s/_search" type)
              q   ((if bound? bound-query unbound-query) pattern)]
          (some->> (request! client :get url q)
                   :body
                   :hits
                   :hits
                   (map (comp :path :_source))))))))
