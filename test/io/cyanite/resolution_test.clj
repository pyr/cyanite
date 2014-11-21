(ns io.cyanite.resolution-test
  (:require [io.cyanite.resolution :refer :all]
            [clojure.test          :refer :all])
  (:import clojure.lang.ExceptionInfo
           io.cyanite.resolution.Resolution))

(deftest is-seconds-test
  (testing "15s becomes 15"
    (is (= 15 (to-seconds "15s"))))

  (testing "36m becomes 2160"
    (is (= 2160 (to-seconds "36m"))))

  (testing "2h becomes 7200"
    (is (= 7200 (to-seconds "2h"))))

  (testing "5d becomes 432000"
    (is (= 432000 (to-seconds "5d"))))

  (testing "2w becomes 1209600"
    (is (= 1209600 (to-seconds "2w"))))

  (testing "1y becomes 31536000"
    (is (= 31536000 (to-seconds "1y"))))

  (testing "unknown unit throws exception"
    (is (thrown? ExceptionInfo (to-seconds "2a")))))

(deftest convert-shorthand-test
  (testing "non-string is left alone"
    (is (= (map->Resolution {:anything true})
           (any->Resolution {:anything true}))))

  (testing "15s:6h is converted correctly"
    (is (= (Resolution. 15 1440)
           (any->Resolution "15s:6h"))))

  (testing "sequential spec"
    (is (= (Resolution. 15 1440) (any->Resolution [15 1440]))))

  (testing "invalid specs"
    (is (thrown-with-msg? ExceptionInfo #"invalid resolution spec"
                          (any->Resolution 1)))
    (is (thrown-with-msg? ExceptionInfo #"invalid resolution spec"
                          (any->Resolution [:a :b :c])))
    (is (thrown-with-msg? ExceptionInfo #"invalid resolution spec"
                          (any->Resolution :foo)))))
