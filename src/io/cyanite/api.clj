(ns io.cyanite.api
  "Cyanite's 'HTTP interface"
  (:require [com.stuartsierra.component :as component]
            [com.climate.claypoole      :as cp]
            [cheshire.core              :as json]
            [ring.util.codec            :as codec]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.engine.buckets  :as b]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [qbits.jet.server           :refer [run-jetty]]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent! now!]]
            [clojure.tools.logging      :refer [info debug error]]
            [clojure.string             :refer [lower-case blank?]]))

(defn parse-time
  "Parse an epoch into a long"
  [time-string]
  (try (Long/parseLong time-string)
       (catch NumberFormatException _
         (error "wrong time format: " time-string)
         nil)))

(def routes
  "Dead simple router"
  [[:paths #"^/paths.*"]
   [:metrics #"^/metrics.*"]
   [:ping  #"^/ping/?"]])

(defn assoc-params
  "Parse query args"
  [{:keys [query-string] :as request}]
  (let [kwm (comp (partial reduce merge {})
                  (partial remove (comp nil? first))
                  (partial map (juxt (comp keyword lower-case key) val)))]
    (if-let [params (and (seq query-string)
                         (codec/form-decode query-string))]
      (assoc request
             :params (kwm (cond (map? params)    params
                                (string? params) {params nil}
                                :else            {})))
      (assoc request :params {}))))

(defn match-route
  "Predicate which returns the matched elements"
  [{:keys [uri path-info] :as request} [action re]]
  (when (re-matches re (or path-info uri))
    action))

(defn assoc-route
  "Find which route matches if any and store the appropriate action
   in the :action field of the request"
  [request]
  (assoc request :action (some (partial match-route request) routes)))

(defmulti dispatch
  "Dispatch on action, as found by assoc-route"
  :action)

(defn process
  "Process a request. Handle errors and report them"
  [request store index engine]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body    (json/generate-string
               (dispatch (assoc request
                                :store  store
                                :index  index
                                :engine engine)))}
    (catch Exception e
      (let [{:keys [status body suppress? exception]} (ex-data e)]
        (when-not suppress?
          (error e "could not process request")
          (when exception
            (error exception "request process exception")))
        {:status (or status 500)
         :headers {"Content-Type" "application/json"}
         :body    (json/generate-string
                   {:message (or body (.getMessage e))})}))))

(defmethod dispatch :ping
  [_]
  {:response "pong"})

(defmethod dispatch :paths
  [{{:keys [query]} :params index :index}]
  (index/prefixes index (if (blank? query) "*" query)))

(defmethod dispatch :metrics
  [{{:keys [from to path agg]} :params :keys [index store engine]}]
  (let [from  (or (parse-time from)
                  (throw (ex-info "missing from parameter"
                                  {:suppress? true :status 400})))
        to    (or (parse-time to) (now!))
        paths (->> (mapcat (partial index/leaves index)
                           (if (sequential? path) path [path]))
                   (map (partial engine/resolution engine from))
                   (remove nil?))]
    (store/query! store from to paths)))

(defmethod dispatch :default
  [_]
  (throw (ex-info "unknown action" {:status 404 :suppress? true})))

(defn make-handler
  "Yield a ring-handler for a request"
  [store index engine]
  (fn [request]
    (-> request
        (assoc-params)
        (assoc-route)
        (process store index engine))))

(defrecord Api [options pool service store index engine]
  component/Lifecycle
  (start [this]
    (let [pool    (cp/threadpool 1)
          handler (make-handler store index engine)]
      (cp/future pool (run-jetty (assoc options :ring-handler handler)))
      (assoc this :pool pool)))
  (stop [this]
    (cp/shutdown pool)
    (dissoc this :pool)))
