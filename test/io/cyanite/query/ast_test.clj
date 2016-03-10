(ns io.cyanite.query.ast-test
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.ast :refer :all]
            [clojure.test :refer :all]))

(defn runq
  [query-string series]
  (run-query! (parser/query->tokens query-string)
              series))

(deftest test-sum-series
  (is (= (runq "sumSeries(f*)"
               {"f*" [[1 1 1]
                      [2 2 2]]})
         ["sumSeries(f*)"
          [[3 3 3]]])))

(deftest test-derivative
  (is (= (runq "derivative(a.b.c)"
               {"a.b.c" [[1 3 6]]})
         ["derivative(a.b.c)"
          [[nil 2 3]]])))
