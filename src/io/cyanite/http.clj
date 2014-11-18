(ns io.cyanite.http
  "Very simple asynchronous HTTP API, implements two
   routes: paths and metrics to query existing paths
   and retrieve metrics"
  (:require [qbits.jet.server      :as http]
            [ring.util.codec       :as codec]
            [io.cyanite.store      :as store]
            [io.cyanite.index      :as index]
            [io.cyanite.resolution :as r]
            [cheshire.core         :as json]
            [clojure.string        :as str]
            [clojure.string        :refer [lower-case]]
            [clojure.tools.logging :refer [info error debug]]))

(def
  ^{:doc "Our dead simple router"}
  routes [[:paths   #"^/paths.*"]
          [:metrics #"^/metrics.*"]
          [:ping #"^/ping/?"]])

(defn now
  "Returns a unix epoch"
  []
  (quot (System/currentTimeMillis) 1000))

(defn keywordized
  "Yield a map where string keys are keywordized"
  [params]
  (dissoc
   (->> (map (juxt (comp keyword lower-case key) val) params)
        (reduce merge {}))
   nil))

(defn assoc-params
  "Parse query args"
  [{:keys [query-string] :as request}]
  (or
   (when-let [params (and (seq query-string)
                          (codec/form-decode query-string))]
     (assoc request
       :params (keywordized
                (cond (map? params) params
                      (string? params) {params nil}
                      :else {}))))
   (assoc request :params {})))

(defn match-route
  [{:keys [uri path-info] :as request} [action re]]
  (when (re-matches re (or path-info uri))
    action))

(defn assoc-route
  "Find matching route in router, store "
  [request]
  (assoc request :action (some (partial match-route request) routes)))

(defmulti process
  "Process request by dispatching on the action that was found"
  :action)

(defmethod process :paths
  [{{:keys [query]} :params :keys [index] :as request}]
  (debug "query now: " query)
  (index/prefixes index "" (if (str/blank? query) "*" query)))

(defmethod process :metrics
  [{{:keys [from to path agg]} :params :keys [index store resolutions]}]
  (debug "fetching paths: " path)
  (let [to    (if to (Long/parseLong to) (now))
        from  (Long/parseLong from)
        paths (mapcat (partial index/lookup index "")
                      (if (sequential? path) path [path]))
        agg   (keyword (or agg "mean"))
        spec  (r/->FetchSpec agg paths from to)]
    (debug "spec: " (pr-str spec))
    (store/fetch store "" spec)))

(defmethod process :ping
  [_]
  {})

(defmethod process :default
  [_]
  (throw (ex-info "unknown action" {:status 404 :suppress? true})))

(defn wrap-process
  "Process request, generating a JSON output for it, catch exception
   and yield a payload"
  [request store index]
  (debug "got request: " request)
  (try
    {:status  200
     :headers {"Content-Type" "application/json"}
     :body    (json/generate-string
               (process (assoc request
                          :store store
                          :rollups (:rollups store)
                          :index index)))}
    (catch Exception e
      (let [{:keys [status body suppress?]} (ex-data e)]
        (when-not suppress?
          (error e "could not process request"))
        {:status (or status 500)
         :headers {"Content-Type" "application/json"}
         :body    (json/generate-string
                   (or body {:error (.getMessage e)}))}))))

(defn server
  "Start the API, handling each request by parsing parameters and
   routes then handing over to the request processor"
  [{:keys [http store index] :as config}]
  (let [handler (fn [request]
                  (-> request
                      (assoc-params)
                      (assoc-route)
                      (wrap-process store index)))]
    (http/run-jetty (merge http {:ring-handler handler})))
  nil)
