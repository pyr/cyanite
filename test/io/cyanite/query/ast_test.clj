(ns io.cyanite.query.ast-test
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.ast :refer :all]
            [clojure.test :refer :all]))

(defn runq
  [query-string series]
  (run-query! (parser/query->tokens query-string)
              series))

(deftest test-sum-series
  (is (= (runq "sumSeries(a.b.c,a.b.d)"
               {"a.b.c" [1 1 1]
                "a.b.d" [2 2 2]})
         [["sumSeries(a.b.c,a.b.d)"
           [3 3 3]]]))
  (is (= (runq "sumSeries(a.b.*)"
               {"a.b.c" [1 1 1]
                "a.b.d" [2 2 2]})
         [["sumSeries(a.b.c,a.b.d)" [3 3 3]]])))

(deftest test-sum-absolute-series
  (is (= (runq "sumSeries(absolute(a.b.c),a.b.d)"
               {"a.b.c" [1 1 1]
                "a.b.d" [2 2 2]})
         [["sumSeries(absolute(a.b.c),a.b.d)"
           [3 3 3]]]))
  (is (= (runq "sumSeries(a.b.*)"
               {"a.b.c" [1 1 1]
                "a.b.d" [2 2 2]})
         [["sumSeries(a.b.c,a.b.d)"
           [3 3 3]]])))

(deftest test-derivative
  (is (= (runq "derivative(a.b.c)"
               {"a.b.c" [1 3 6]})
         [["derivative(a.b.c)"
           [nil 2 3]]]))
  (is (= (runq "derivative(a.b.*)"
               {"a.b.c" [1 3 6]
                "a.b.d" [1 3 6]})
         [["derivative(a.b.c)"
           [nil 2 3]]
          ["derivative(a.b.d)"
           [nil 2 3]]])))

(deftest test-absolute
  (is (= (runq "absolute(a.b.c)"
               {"a.b.c" [-1 -3 -6]})
         [["absolute(a.b.c)"
           [1 3 6]]]))
  (is (= (runq "absolute(a.b.*)"
               {"a.b.c" [-1 -3 -5]
                "a.b.d" [-1 -3 -6]})
         [["absolute(a.b.c)" [1 3 5]]
          ["absolute(a.b.d)" [1 3 6]]])))

(deftest test-scale
  (is (= (runq "scale(a.b.c,10.0)"
               {"a.b.c" [1 2 3]})
         [["scale(a.b.c,10.0)"
           [10.0 20.0 30.0]]]))
  (is (= (runq "scale(a.b.*,10.0)"
               {"a.b.c" [1 2 3]
                "a.b.d" [5 6 7]})
         [["scale(a.b.c,10.0)"
           [10.0 20.0 30.0]]
          ["scale(a.b.d,10.0)"
           [50.0 60.0 70.0]]])))

(deftest test-div
  (is (= (runq "divideSeries(a.b.c,a.b.d)"
               {"a.b.c" [10 20 30]
                "a.b.d" [2 4 6]})
         [["divideSeries(a.b.c,a.b.d)"
           [5 5 5]]])))

(deftest test-path
  (is (= (runq "a.b.*"
               {"a.b.c" [10 20 30]
                "a.b.d" [2 4 6]})
         [["a.b.c" [10 20 30]]
          ["a.b.d" [2 4 6]]]))
  (is (= (runq "a.b.c"
               {"a.b.c" [10 20 30]
                "a.b.d" [2 4 6]})
         [["a.b.c" [10 20 30]]])))
