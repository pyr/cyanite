(ns io.cyanite.integration.index-test
  (:require [io.cyanite.store           :refer :all]
            [clojure.test               :refer :all]
            [io.cyanite.test-helper     :refer :all]
            [io.cyanite.index           :as index]
            [qbits.alia                 :as alia]))

(defn cleanup-tables
  [session]
  (alia/execute session (str "TRUNCATE TABLE segment")))

(defn test-index
  []
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
      (is (= #{"aa.bb.cc" "aa.bb.dd" "cc.bb.cc" "cc.bb.dd"}
             (set (map :id (index/prefixes index "*.bb.*")))))
      (is (= #{"aa.bb.dd" "cc.bb.dd"}
             (set (map :id (index/prefixes index "*.*.dd")))))
      (is (= #{"cc.bb.cc" "cc.ee.cc" "cc.ff.cc" "cc.gg.cc" "cc.hh.cc"}
             (set (map :id (index/prefixes index "cc.*.cc")))))
      (is (= #{"aa.ee.cc" "cc.ee.cc"}
             (set (map :id (index/prefixes index "*.ee.cc")))))
      (is (= #{"aa" "cc"}
             (set (map :id (index/prefixes index "*")))))
      (is (= #{"aa.ee.cc" "aa.hh.cc" "aa.bb.cc" "aa.ff.cc" "aa.gg.cc"}
             (set (map :id (index/prefixes index "aa.*.cc")))))
      (is (= #{"aa.ee.cc"}
             (set (map :id (index/prefixes index "aa.ee.cc")))))
      (cleanup-tables session)))

(deftest sasi-index-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :cassandra
             :cluster  "localhost"
             :keyspace "cyanite_test"}}
    {}
    (test-index)))

(deftest agent-index-test
  (with-config
    {:store {:cluster  "localhost"
             :keyspace "cyanite_test"}
     :index {:type     :atom}}
    {}
    (test-index)))
