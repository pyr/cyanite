(ns io.cyanite.integration.index-test
  (:require [io.cyanite.store           :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]
            [clj-time.core :as t]))

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
      (index/register! index "aa.bb.cc")
      (index/register! index "aa.bb.dd")
      (index/register! index "aa.ee.cc")
      (index/register! index "aa.ff.cc")
      (index/register! index "aa.gg.cc")
      (index/register! index "aa.hh.cc")
      (index/register! index "cc.bb.cc")
      (index/register! index "cc.bb.dd")
      (index/register! index "cc.ee.cc")
      (index/register! index "cc.ff.cc")
      (index/register! index "cc.gg.cc")
      (index/register! index "cc.hh.cc")

      (is (= #{"aa.bb" "aa.ee" "aa.ff" "aa.gg" "aa.hh"}
             (set (map :id (index/prefixes index "aa.*")))))
      (is (= #{}
             (set (map :id (index/prefixes index "bb.*")))))
      (is (= #{"aa.bb" "cc.bb"}
             (set (map :id (index/prefixes index "*.bb")))))
      (is (= #{"aa" "cc"}
             (set (map :id (index/prefixes index "*")))))
      (is (= #{"aa.ee.cc" "aa.hh.cc" "aa.bb.cc" "aa.ff.cc" "aa.gg.cc"}
             (set (map :id (index/prefixes index "aa.*.cc")))))
      (is (= #{"aa.ee.cc"}
             (set (map :id (index/prefixes index "aa.ee.cc")))))
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
