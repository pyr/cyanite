(ns org.spootnik.cyanite.http
  "Very simple asynchronous HTTP API, implements two
   routes: paths and metrics to query existing paths
   and retrieve metrics"
  (:require [aleph.http                 :as http]
            [ring.util.codec            :as codec]
            [org.spootnik.cyanite.store :as store]
            [cheshire.core              :as json]
            [lamina.core                :refer [enqueue]]
            [clojure.string             :refer [lower-case]]
            [clojure.tools.logging      :refer [info error debug]]))

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

(defn find-best-rollup
  "Find most precise storage period given the oldest point wanted"
  [from rollups]
  (let [within (fn [{:keys [rollup period] :as rollup-def}]
                 (and (>= (Long/parseLong from) (- (now) (* rollup period)))
                      rollup-def))]
    (some within (sort-by :rollup rollups))))

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
  ""
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
  [{{:keys [query]} :params :as request}]
  (store/find-prefixes query))

(defmethod process :metrics
  [{{:keys [from to path agg]} :params :keys [store rollups] :as request}]
  (debug "fetching paths: " path)
  (if-let [{:keys [rollup period]} (find-best-rollup from rollups)]
    (let [to    (if to (Long/parseLong to) (now))
          from  (Long/parseLong from)
          paths (mapcat (partial store/find-paths false)
                        (if (sequential? path) path [path]))]
      (store/fetch store (or agg "mean") paths rollup period from to))
    {:step nil :from nil :to nil :series {}}))

(defmethod process :ping
  [_]
  {})

(defmethod process :default
  [_]
  (throw (ex-info "unknown action" {:status 404 :suppress? true})))

(defn wrap-process
  "Process request, generating a JSON output for it, catch exception
   and yield a payload"
  [request rollups chan store]
  (debug "got request: " request)
  (enqueue
   chan
   (try
     {:status  200
      :headers {"Content-Type" "application/json"}
      :body    (json/generate-string
                (process (assoc request :store store :rollups rollups)))}
     (catch Exception e
       (let [{:keys [status body suppress?]} (ex-data e)]
         (when-not suppress?
           (error e "could not process request"))
         {:status (or status 500)
          :headers {"Content-Type" "application/json"}
          :body    (json/generate-string
                    (or body {:error (.getMessage e)}))})))))

(defn start
  "Start the API, handling each request by parsing parameters and
   routes then handing over to the request processor"
  [{:keys [http store carbon] :as config}]
  (let [handler (fn [chan request]
                  (-> request
                      (assoc-params)
                      (assoc-route)
                      (wrap-process (:rollups carbon) chan store)))]
    (http/start-http-server handler http))
  nil)
