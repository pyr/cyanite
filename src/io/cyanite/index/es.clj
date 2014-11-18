(ns io.cyanite.index.es
  (:require [io.cyanite.index      :as index]
            [cheshire.core         :as json]
            [qbits.jet.client.http :as http]
            [clojure.core.async    :as a]
            [clojure.tools.logging :refer [debug]]))

(def headers {"content-type" "application/json"})

(defn es-index
  [{:keys [url index] :or {url "http://localhost:9200" index "paths"}}]
  (let [client (http/client)]
    (reify
      index/Index
      (register! [this tenant path]
        (http/post client (format "%s/%s/path" url index)
                   {:headers headers
                    :as      :json
                    :body    (json/generate-string
                              {:path   path
                               :tenant tenant})}))
      (query [this tenant path recurse?]
        (let [q   {:query {:bool {:must [{:term {:tenant tenant}}
                                         {:wildcard {:path path}}]}}}
              url (format "%s/%s/path/_search" url index)
              c   (http/post client url {:headers headers
                                         :as      :json
                                         :body    (json/generate-string q)})
              res (a/<!! c)]
          (when-let [body (and (= (quot (:status res) 100) 2)
                               (a/<!! (:body res)))]
            (->> body
                 :hits
                 :hits
                 (mapv #(get-in % [:_source :path])))))))))

(comment
  (def j (es-index {}))
  (def i (index/wrapped-index j))

  (do
    (index/register! j "" "web01.cpu.idle")
    (index/register! j "" "web01.cpu.system")
    (index/register! j "" "web01.cpu.user")
    (index/register! j "" "web01.cpu.irq")
    (index/register! j "" "web02.cpu.idle")
    (index/register! j "" "web02.cpu.system")
    (index/register! j "" "web02.cpu.user")
    (index/register! j "" "web02.cpu.irq")
    (index/register! j "meh" "web01.cpu.idle")
    (index/register! j "meh" "web01.cpu.system")
    (index/register! j "meh" "web01.cpu.user")
    (index/register! j "meh" "web01.cpu.irq")
    (index/register! j "meh" "web02.cpu.idle")
    (index/register! j "meh" "web02.cpu.system")
    (index/register! j "meh" "web02.cpu.user")
    (index/register! j "meh" "web02.cpu.irq")
    (index/register! j "meh" "foo")
    (index/register! j "meh" "foo"))

  (index/query j "meh" "foo*" true)
  (index/prefixes i "" "web*.cpu.*")

  (index/query j "" "web0*.cpu.idle" true)
  (index/lookup i "" "web0*.cpu.idle")
  (index/query i "meh" "*.cpu.idle"))
