(ns io.cyanite.es-client
  "Hacks of elastich to use async http-kit"
  (:require [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.rest.bulk :as esrb]
            [clojurewerkz.elastisch.rest :as rest]
            [clojurewerkz.elastisch.arguments :as ar]
            [clojure.string :as str]
            [qbits.jet.client.http :as http]
            [cheshire.core :as json]
            [clojure.core.async :refer [<! go]]
            [clojure.tools.logging :refer [error info debug]])
  (:import clojurewerkz.elastisch.rest.Connection))

(def buf-size (* 4096 1024))

(def client (delay (http/client {:response-buffer-size buf-size
                                 :request-buffer-size buf-size})))

(defn multi-get
  [^Connection conn index mapping-type query func]
  (go
    (let [url  (rest/index-mget-url conn index mapping-type)
          resp (<! (http/post @client url
                              {:body (json/encode {:docs query})
                               :fold-chunked-response? true
                               :as :json}))
          body (<! (:body resp))]
      (if (= 200 (:status resp))
        (func (filter :found (:docs body)))
        (error "ES responded with non-200: " body)))))

(defn bulk-with-url
  [url operations func]
  (let [bulk-json (-> (map json/encode operations)
                      (interleave (repeat "\n"))
                      (str/join))]
    (go
      (let [resp   (<! (http/post @client url {:body bulk-json}))
            status (:status resp)]
        (when-not (= 200 status)
          (func resp))))))

(defn remove-ids-from-docs
  [op-or-doc]
  (if (:tenant op-or-doc)
    (dissoc op-or-doc :_id)
    op-or-doc))

(defn multi-update
  [^Connection conn index mapping-type docs func]
  (bulk-with-url (rest/bulk-url conn index mapping-type)
                 (map
                  remove-ids-from-docs
                  (esrb/bulk-index (map #(assoc % :_id (:path %)
                                                :_index index
                                                :_type mapping-type) docs)))
                 func))
