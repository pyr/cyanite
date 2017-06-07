(ns io.cyanite.store
  (:require [com.stuartsierra.component :as component]
            [io.cyanite.store.cassandra :as c]
            [clojure.tools.logging      :refer [info error]]))

(defprotocol MetricStore
  (insert! [this path resolution snapshot])
  (fetch!  [this from to paths])
  (truncate! [this]))

(defn reconstruct-aggregate
  [path aggregate]
  (if (= :default aggregate)
    path
    (str path "_" (name aggregate))))

(defn common-fetch!
  [paths f]
  (let [aggregates (reduce (fn [acc {:keys [aggregate] :as path}]
                             (assoc acc
                                    (dissoc path :aggregate)
                                    (if-let [aggregates (get acc path)]
                                      (conj aggregates aggregate)
                                      [aggregate])))
                           {} paths)
        paths      (keys aggregates)
        results    (f paths)]
    (mapcat
     (fn [{:keys [id point] :as metric}]
       (map #(let [aggregate (if (= :default %) :mean %)]
               (assoc metric
                      :id    (assoc id
                                    :path (reconstruct-aggregate (:path id) %)
                                    :aggregate %)
                      :point (get point aggregate)))
            (get aggregates id)))
     results)))

(defrecord CassandraV2Store [options session insertq fetchq truncateq
                             wrcty rdcty mkid mkpoint reporter
                             statement-cache]
  component/Lifecycle
  (start [this]
    (let [[session rdcty wrcty] (c/session! options)
          table                 (or (:table options) "metric")
          [mkid mkpoint]        (c/get-types session)]
      (-> this
          (assoc :session   session
                 :insertq   (c/insertq-v2 session table)
                 :fetchq    (c/fetchq-v2 session table)
                 :truncateq (c/truncateq-v2 session table)
                 :mkid      mkid
                 :mkpoint   mkpoint))))
  (stop [this]
    (-> this
        (assoc :session nil
               :insertq nil
               :fetchq nil
               :truncateq nil
               :mkid nil
               :mkpoint nil)))
  MetricStore
  (fetch! [this from to paths]
    (common-fetch!
     paths
     (fn [paths]
       (->> paths
            (pmap
             (fn [path]
               (->> (c/runq! session fetchq
                             [(mkid path)
                              (long from)
                              (long to)]
                             {:consistency rdcty
                              :fetch-size  Integer/MAX_VALUE})
                    (map (fn [i] (assoc i :id path))))))
            (mapcat identity)))))

  (insert! [this path resolution snapshot]
    (c/runq-async! session insertq
                   [(-> resolution :period int)
                    (mkpoint snapshot)
                    (mkid {:path path :resolution resolution})
                    (-> snapshot :time long)]
                   {:consistency wrcty}))

  (truncate! [this]
    (c/runq! session truncateq [] {})))

(defn empty-store
  []
  (reify
    component/Lifecycle
    (start [this] this)
    (stop [this] this)
    MetricStore
    (fetch! [this from to paths])
    (insert! [this path resolution snapshot])))

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
    (common-fetch!
     paths
     #(let [st @state]
        (mapcat
         (fn [path]
           (->> (get-in st ((juxt :path :resolution) path))
                (filter
                 (fn [[time _]]
                   (and (>= time from)
                        (<= time to))))
                (map
                 (fn [[time point]]
                   {:id    path
                    :time  time
                    :point point}))))
         %))))
  (insert! [this path resolution snapshot]
    (swap! state
           (fn [old]
             (update-in old
                        [path resolution (:time snapshot)]
                        (constantly (select-keys snapshot [:max :min :sum :mean]))))))
  (truncate! [this]
    (reset! state {})))

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
