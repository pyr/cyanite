(ns org.spootnik.cyanite.carbon-test
  (:require [org.spootnik.cyanite.carbon :refer :all]
            [org.spootnik.cyanite.config :refer [assoc-rollup-to]]
            [clojure.test                :refer :all]))

(deftest formatter-test
  (testing "empty-transform"
    (is (= nil (formatter [] "foo nan 501"))))

  (testing "rollup-transform"
    (is (= (list {:path "foo.bar"
                  :rollup 10
                  :period 3600
                  :ttl    36000
                  :time   500
                  :metric 2.0
                  :tenant "NONE"})
           (formatter
                      (assoc-rollup-to  [{:rollup 10 :period 3600}])
                      "foo.bar 2.0 501")))

    (is (= (list {:path "foo.bar"
                  :rollup 10
                  :period 3600
                  :ttl    36000
                  :time   600
                  :metric 2.0
                  :tenant "NONE"}
                 {:path "foo.bar"
                  :rollup 60
                  :period 7200
                  :ttl    432000
                  :time   600
                  :metric 2.0
                  :tenant "NONE"})
           (formatter
                      (assoc-rollup-to  [{:rollup 10 :period 3600}
                                         {:rollup 60 :period 7200}])
                      "foo.bar 2.0 601")))


    )
  (testing "rollup-transform-tenants"
    (is (= (list {:path "foo.bar"
                  :rollup 10
                  :period 3600
                  :ttl    36000
                  :time   500
                  :metric 2.0
                  :tenant "example_tenant"})
           (formatter
             (assoc-rollup-to  [{:rollup 10 :period 3600}])
             "foo.bar 2.0 501 example_tenant")))

    (is (= (list {:path "foo.bar"
                  :rollup 10
                  :period 3600
                  :ttl    36000
                  :time   600
                  :metric 2.0
                  :tenant "example_tenant"}
                 {:path "foo.bar"
                  :rollup 60
                  :period 7200
                  :ttl    432000
                  :time   600
                  :metric 2.0
                  :tenant "example_tenant"})
           (formatter
             (assoc-rollup-to  [{:rollup 10 :period 3600}
                                {:rollup 60 :period 7200}])
             "foo.bar 2.0 601 example_tenant")))
    ))
