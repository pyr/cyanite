(ns io.cyanite.api
  "Cyanite's 'HTTP interface"
  (:require [com.stuartsierra.component :as component]
            [cheshire.core              :as json]
            [clj-time.core              :as t]
            [io.cyanite.engine.rule     :as rule]
            [io.cyanite.engine          :as engine]
            [io.cyanite.engine.queue    :as q]
            [io.cyanite.index           :as index]
            [io.cyanite.store           :as store]
            [io.cyanite.query           :as query]
            [io.cyanite.http            :as http]
            [io.cyanite.engine.drift    :refer [epoch!]]
            [io.cyanite.utils           :refer [nbhm assoc-if-absent!]]
            [clj-time.coerce            :refer [to-epoch]]
            [clojure.tools.logging      :refer [info debug error]]
            [clojure.string             :refer [lower-case blank?]]))


(defn sub-time
  [s]
  (let [[_ value unit] (re-find #"^([0-9]+)([a-z])" s)]
    (when-not (and value unit)
      (throw (ex-info (str "invalid time interval: " s) {:value value
                                                         :unit unit})))
    (when (and value unit)
      (to-epoch
       (t/minus (t/now)
                ((case unit
                   "s" t/seconds
                   "m" t/minutes
                   "h" t/hours
                   "d" t/days
                   "w" t/weeks
                   t/seconds)
                 (Long/valueOf value)))))))

(defn parse-time
  "Parse an epoch into a long"
  [time-string drift]
  (cond
    (nil? time-string)
    nil

    (= time-string "now")
    (epoch! drift)

    (.startsWith time-string "-")
    (sub-time (.substring time-string 1))


    :else
    (try (Long/parseLong time-string)
         (catch NumberFormatException _
           (error "wrong time format: " time-string)
           nil))))

(def routes
  "Dead simple router"
  [[:paths   #"^/paths.*"]
   [:paths   #"^/metrics/find"]
   [:metrics #"^/metrics.*"]
   [:render  #"^/render"]
   [:ping    #"^/ping/?"]])

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
  [request store index engine drift]
  (try
    {:status 200
     :headers {"Content-Type" "application/json"}
     :body    (json/generate-string
               (dispatch (assoc request
                                :store  store
                                :index  index
                                :drift  drift
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
  (debug "path fetch request for:" (pr-str query))
  (when (sequential? query)
    (throw (ex-info "only one query argument supported for path queries"
                    {:status 400 :suppress? true})))
  (let [[path _] (if (blank? query)
                    "*"
                    (index/extract-aggregate index query))]
    (->> path
         (index/prefixes index)
         (index/multiplex-aggregates index))))

(defmethod dispatch :render
  [{{:keys [from until target format]} :params :keys [index drift store engine]}]
  (when (not= format "json")
    (throw (ex-info "Cyanite only outputs JSON for now"
                    {:suppress? true :status 400})))
  (let [from  (or (parse-time from drift)
                  (throw (ex-info "missing from parameter"
                                  {:suppress? true :status 400})))
        to    (or (parse-time until drift) (epoch! drift))]
    (query/run-query! store index engine from to
                      (if (seq? target)
                        target
                        [target]))))

(defmethod dispatch :metrics
  [{{:keys [from to path]} :params :keys [index store engine drift]}]
  (debug "metric fetch request for:" (pr-str path))
  (let [from  (or (parse-time from drift)
                  (throw (ex-info "missing from parameter"
                                  {:suppress? true :status 400})))
        to    (or (parse-time to drift) (epoch! drift))
        paths (->> (if (sequential? path) path [path])
                   (map #(index/extract-aggregate index %))
                   (mapcat (fn [[path aggregate]]
                             (->> path
                                  (index/prefixes index)
                                  (map #(vector % aggregate)))))
                   (map (fn [[path aggregate]]
                          (engine/resolution engine from to (:path path) aggregate)))
                   (remove nil?))]
    (store/query! store from to paths)))

(defmethod dispatch :default
  [_]
  (throw (ex-info "unknown action" {:status 404 :suppress? true})))

(defn make-handler
  "Yield a ring-handler for a request"
  [store index engine drift]
  (fn [request]
    (debug "got request: " (pr-str request))
    (-> request
        (assoc-route)
        (process store index engine drift))))

(defrecord Api [options server service store index engine drift]
  component/Lifecycle
  (start [this]
    (if (:disabled options)
      this
      (let [handler (make-handler store index engine drift)
            server  (http/run-server options handler)]
        (assoc this :server server))))
  (stop [this]
    (when (fn? server)
      (server))
    (assoc this :server nil)))
