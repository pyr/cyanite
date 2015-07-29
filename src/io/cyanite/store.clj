(ns io.cyanite.store
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.store.pure      :as p]
            [clojure.tools.logging      :refer [info error]]))

(defprotocol MetricStore
  (insert! [this metric])
  (fetch!  [this from to paths]))

(defrecord CassandraV2Store [options session insertq fetchq
                             wrcty rdcty mkid mkpoint]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)
          table                 (or (:table options) "metric")
          [mkid mkpoint]        (c/get-types session)]
      (-> this
          (assoc :session session)
          (assoc :insertq (c/insertq-v2 session table))
          (assoc :fetchq  (c/fetchq-v2 session table))
          (assoc :mkid mkid)
          (assoc :mkpoint mkpoint))))
  (stop [this]
    (-> this
        (dissoc :session)
        (dissoc :inserq!)
        (dissoc :fetchq)
        (dissoc :mkid)
        (dissoc :mkpoint)))
  MetricStore
  (fetch! [this from to paths]
    (c/runq! session fetchq
             [(mapv mkid paths)
              (long from)
              (long to)]
             {:consistency rdcty
              :fetch-size Integer/MAX_VALUE}))

  (insert! [this metric]
    (c/runq! session insertq
             [(-> metric :resolution :period int)
              (mkpoint metric)
              (mkid metric)
              (-> metric :time long)]
             {:consistency wrcty})))

(defmulti build-store (comp (fnil keyword "cassandra-v2") :type))

(defmethod build-store :cassandra-v2
  [options]
  (map->CassandraV2Store {:options (dissoc options :type)}))

(defn query! [store from to paths]
  (let [raw-series         (fetch! store from to paths)
        [precision series] (p/normalize raw-series)]
    (p/data->series series to precision)))
