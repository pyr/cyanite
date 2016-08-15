(ns io.cyanite.integration.index-test
  (:require [io.cyanite.store           :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]))

(defn cleanup-tables
  [session]
  (doseq [table ["metric" "path" "segment"]]
    (alia/execute session (str "TRUNCATE TABLE " table))))

(deftest index-prefixes-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          session  (:session store)]
      (cleanup-tables session)
      (index/register! index "a.b.c")
      (index/register! index "a.b.d")
      (index/register! index "a.e.c")
      (index/register! index "a.f.c")
      (index/register! index "a.g.c")
      (index/register! index "a.h.c")

      (is (= ["a.b" "a.e" "a.f" "a.g" "a.h"]
             (map :id (index/prefixes index "a.*"))))
      (is (= []
             (map :id (index/prefixes index "b.*"))))
      (cleanup-tables session))))

(deftest leaves-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)
          store    (:store *system*)
          session  (:session store)]
      (cleanup-tables session)
      (index/register! index "a.b.c")
      (index/register! index "a.b.d")
      (index/register! index "a.e.c")
      (index/register! index "a.f.c")
      (index/register! index "a.g.c")
      (index/register! index "a.h.c")

      (is (= ["a.b.c" "a.b.d"]
             (map :path (index/leaves index "a.b.*"))))
      (is (= ["a.e.c"]
             (map :path (index/leaves index "a.e.*"))))
      (is (= []
             (map :path (index/leaves index "a.z.*"))))
      (cleanup-tables session))))
