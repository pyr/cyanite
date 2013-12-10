(ns so.grep.cyanite.carbon-test
  (:require [so.grep.cyanite.carbon :refer :all]
            [clojure.test           :refer :all]))

(deftest formatter-test
  (testing "nil-transform"
    (is (= nil (formatter [] "foo nan 1234")))))
