(ns io.cyanite.store
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.store.pure      :as p]
            [clojure.tools.logging      :refer [info error]]))

(defprotocol MetricStore
  (insert! [this metric])
  (fetch!  [this from to paths]))

(defrecord MemoryStore [db]
  component/Lifecycle
  (start [this]
    (assoc this :db (atom nil)))
  (stop [this]
    (dissoc this :db))
  MetricStore
  (insert! [this metric]
    (swap! db update (:path metric) conj metric)))

(defrecord CassandraV2Store [options session insertq fetchq
                             wrcty rdcty id-type point-type]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)
          table                 (or (:table options) "metric")
          ks                    (-> session
                                    .getCluster
                                    .getMetadata
                                    (.getKeyspace "metric"))]
      (-> this
          (assoc :session session)
          (assoc :insertq (c/insertq-v2 session table))
          (assoc :fetchq  (c/fetchq-v2 session table))
          (assoc :id-type (.getUserType ks "metric_id"))
          (assoc :point-type (.getUserType ks "metric_point")))))
  (stop [this]
    (-> this
        (dissoc :session)
        (dissoc :inserq!)
        (dissoc :fetchq)
        (dissoc :id-type)
        (dissoc :point-type)))
  MetricStore
  (fetch! [this from to paths]
    (c/runq! session fetchq
                   [(mapv (partial c/->id id-type) paths)
                    (long from)
                    (long to)]
                   {:consistency rdcty
                    :fetch-size Integer/MAX_VALUE}))

  (insert! [this metric]
    (c/runq! session insertq
             [(-> metric :resolution :period int)
              (c/->point point-type metric)
              (c/->id id-type metric)
              (-> metric :time long)]
             {:consistency wrcty})))

(defmulti build-store (comp (fnil keyword "cassandra-v2") :type))

(defmethod build-store :memory
  [options]
  (MemoryStore. nil))

(defmethod build-store :cassandra-v2
  [options]
  (map->CassandraV2Store {:options (dissoc options :type)}))
