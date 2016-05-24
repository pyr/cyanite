(ns io.cyanite.integration.index-test
  (:require [io.cyanite.store           :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]))

(defn cleanup-tables
  [f]
  (f)
  (for [table ["metric" "path" "segment"]]
    (alia/execute (str "TRUNCATE TABLE " table))))

(use-fixtures :each cleanup-tables)

(deftest index-prefixes-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)]
      (index/register! index "a.b.c")
      (index/register! index "a.b.d")
      (index/register! index "a.e.c")
      (index/register! index "a.f.c")
      (index/register! index "a.g.c")
      (index/register! index "a.h.c")

      (is (= ["a.b" "a.e" "a.f" "a.g" "a.h"]
             (map :id (index/prefixes index "a.*"))))
      (is (= []
             (map :id (index/prefixes index "b.*")))))))

(deftest leaves-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (let [index    (:index *system*)]
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
             (map :path (index/leaves index "a.z.*")))))))
