(ns org.spootnik.cyanite.http
  "Very simple asynchronous HTTP API, implements two
   routes: paths and metrics to query existing paths
   and retrieve metrics"
  (:use net.cgrand.moustache)
  ;(:use lamina.core)
  (:use aleph.http)
  (:use ring.middleware.json)
  (:use ring.middleware.params)
  (:use ring.middleware.keyword-params)
  (:require [ring.util.codec            :as codec]
            [org.spootnik.cyanite.store :as store]
            [org.spootnik.cyanite.path  :as path]
            [cheshire.core              :as json]
            [clojure.string             :as str]
            [lamina.core                :refer [enqueue]]
            [clojure.string             :refer [lower-case]]
            [clojure.tools.logging      :refer [info error debug]]))


(defn now
  "Returns a unix epoch"
  []
  (quot (System/currentTimeMillis) 1000))


(defn find-best-rollup
  "Find most precise storage period given the oldest point wanted"
  [from rollups]
  (let [within (fn [{:keys [rollup period] :as rollup-def}]
                 (and (>= (Long/parseLong from) (- (now) (* rollup period)))
                      rollup-def))]
    (some within (sort-by :rollup rollups))))


(defn wrap-local-params [handler params]
  "Adds additional parameters to request"
  (fn [request]
    (handler (assoc request :local-params params))))

(defn paths-handler [response-channel {{:keys [query tenant]}  :params
                                       {:keys [index]} :local-params}]
  (debug "query now: " query)
  (enqueue
    response-channel
    (try
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
               (path/prefixes index (or tenant "NONE") (if (str/blank? query) "*" query)))}
      (catch Exception e
        (let [{:keys [status body suppress?]} (ex-data e)]
          (when-not suppress?
            (error e "could not process request"))
          {:status (or status 500)
           :headers {"Content-Type" "application/json"}
           :body    (json/generate-string
                      (or body {:error (.getMessage e)}))})))))

(defn metrics-handler [response-channel
                       {{:keys [index store rollups]} :local-params
                        {:keys [from to path agg tenant]} :params :as request}]
  (debug "fetching paths: " path)
  (enqueue
    response-channel
    (try
      {:status 200
       :headers {"Content-Type" "application/json"}
       :body (json/generate-string
               (if-let [{:keys [rollup period]} (find-best-rollup (str from) rollups)]
                 (let [to    (if to (Long/parseLong (str to)) (now))
                       from  (Long/parseLong (str from))
                       paths (mapcat (partial path/lookup index (or tenant "NONE"))
                                     (if (sequential? path) path [path]))]
                   (store/fetch store (or agg "mean") paths (or tenant "NONE") rollup period from to))
                 {:step nil :from nil :to nil :series {}})
               )}
      (catch Exception e
        (let [{:keys [status body suppress?]} (ex-data e)]
          (when-not suppress?
            (error e "could not process request"))
          {:status (or status 500)
           :headers {"Content-Type" "application/json"}
           :body    (json/generate-string
                      (or body {:error (.getMessage e)}))})))))

(def handler
  (app
    ["ping"] {:get "OK"}
    ["metrics"] {:any (-> metrics-handler
                          (wrap-aleph-handler)
                          (wrap-keyword-params)
                          (wrap-json-params)
                          (wrap-params))}
    ["paths"] {:any (-> paths-handler
                        (wrap-aleph-handler)
                        (wrap-keyword-params)
                        (wrap-json-params)
                        (wrap-params))}
    [&] {:any (json/generate-string {:status "Error" :reason "Unknown action"})}))

(defn start
  "Start the API, handling each request by parsing parameters and
   routes then handing over to the request processor"
  [{:keys [http store carbon index] :as config}]
  ;(let [handler (fn [chan request]
  ;                (-> request
  ;                    (assoc-params)
  ;                    (assoc-route)
  ;                    (wrap-process (:rollups carbon) chan store index)))]
  ;  (start-http-server handler http))
  (start-http-server (wrap-ring-handler  (wrap-local-params handler {:store store
                                                                     :rollups (:rollups carbon)
                                                                     :index index})) http)
  nil)
