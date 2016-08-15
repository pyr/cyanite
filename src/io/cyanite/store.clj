(ns io.cyanite.store
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [io.cyanite.store.pure      :as p]
            [clojure.tools.logging      :refer [info error]]))

(defprotocol MetricStore
  (insert! [this metric])
  (insert-batch! [this metrics])
  (fetch!  [this from to paths]))

(defrecord CassandraV2Store [options session insertq insert-batchq fetchq
                             wrcty rdcty mkid mkpoint reporter]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)
          table                 (or (:table options) "metric")
          batch-size            (or (:batch-size options) 10)
          [mkid mkpoint]        (c/get-types session)]
      (-> this
          (assoc :session session)
          (assoc :insertq (c/insertq-v2 session table))
          (assoc :insert-batchq (c/insert-batchq-v2 session table batch-size))
          (assoc :fetchq  (c/fetchq-v2 session table))
          (assoc :mkid mkid)
          (assoc :mkpoint mkpoint))))
  (stop [this]
    (-> this
        (assoc :session nil)
        (assoc :insertq nil)
        (assoc :insert-batchq nil)
        (assoc :fetchq nil)
        (assoc :mkid nil)
        (assoc :mkpoint nil)))
  MetricStore
  (fetch! [this from to paths]
    (c/runq! session fetchq
             [(mapv mkid paths)
              (long from)
              (long to)]
             {:consistency rdcty
              :fetch-size Integer/MAX_VALUE}))

  (insert! [this metric]
    (c/runq-async! session insertq
                   [(-> metric :resolution :period int)
                    (mkpoint metric)
                    (mkid metric)
                    (-> metric :time long)]
                   {:consistency wrcty}))
  (insert-batch! [this metrics]
    (c/runq-async! session insert-batchq
                   (mapcat identity
                    (for [metric metrics]
                      [(-> metric :resolution :period int)
                       (mkpoint metric)
                       (mkid metric)
                       (-> metric :time long)]))
                   {:consistency wrcty})))

(defn empty-store
  []
  (reify
    component/Lifecycle
    (start [this] this)
    (stop [this] this)
    MetricStore
    (fetch! [this from to paths])
    (insert-batch! [this metric])
    (insert! [this metric])))

(defrecord MemoryStore [state]
  component/Lifecycle
  (start [this]
    (assoc this :state (atom {})))
  (stop [this]
    (assoc this :state nil))
  clojure.lang.IDeref
  (deref [this]
    @state)
  MetricStore
  (fetch! [this from to paths]
    (let [st @state]
      (mapcat
       (fn [path]
         (->> (get st path)
              (filter
               (fn [[time _]]
                 (and (>= time from)
                      (<= time to))))
              (map
               (fn [[time point]]
                 {:id    path
                  :time  time
                  :point point}))))
       paths)))
  (insert-batch! [this metrics]
    (doseq [metric metrics]
      (insert! this metric)))
  (insert! [this metric]
    (swap! state
           (fn [old]
             (update-in old
                        [(select-keys metric [:path :resolution])
                         (:time metric)]
                        (constantly (select-keys metric [:max :min :sum :mean])))))))

(prefer-method print-method clojure.lang.IPersistentMap clojure.lang.IDeref)

(defmulti build-store (comp (fnil keyword "cassandra-v2") :type))

(defmethod build-store :cassandra-v2
  [options]
  (map->CassandraV2Store {:options (dissoc options :type)}))

(defmethod build-store :empty
  [options]
  (empty-store))

(defmethod build-store :memory
  [options]
  (map->MemoryStore options))

(defn query! [store from to paths]
  (let [raw-series         (fetch! store from to paths)
        [precision series] (p/normalize raw-series)]
    (p/data->series series to precision)))


;; TODO IMPLEMENT LAG GAUGE
