(ns io.cyanite.carbon-test
  (:require [io.cyanite.transport.carbon :refer :all]
            [clojure.test      :refer :all]))

(deftest input-test
  (testing "empty-transform"
    (is (= [{:path "foo"
             :point nil
             :time 501}]
           (text->input "foo nan 501"))))

  (testing "normal-transform"
    (is (= [{:path "foo.bar"
             :point 2.0
             :time 501}]
           (text->input "foo.bar 2.0 501")))))
