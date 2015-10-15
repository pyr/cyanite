(ns io.cyanite.dsl-test
  (:require [io.cyanite.query.parser :as parser]
            [io.cyanite.query.path   :as path]
            [clojure.test            :refer :all]))

(deftest ast-test
  (let [inputs [["simple path"
                 "foo"
                 [:path "foo"]
                 #{"foo"}]

                ["wildcard path"
                 "foo.*"
                 [:path "foo.*"]
                 #{"foo.*"}]

                ["absolute"
                 "absolute(foo)"
                 [:absolute [:path "foo"]]
                 #{"foo"}]

                ["aggregate line"
                 "aggregateLine(foo,'max')"
                 [:aggregateline [:path "foo"] "max"]
                 #{"foo"}]

                ["alias"
                 "alias(foo,'bar')"
                 [:alias [:path "foo"] "bar"]
                 #{"foo"}]

                ["alias by metric"
                 "aliasByMetric(foo)"
                 [:aliasmetric [:path "foo"]]
                 #{"foo"}]

                ["alias by node (2-arity)"
                 "aliasByNode(foo,5)"
                 [:aliasnode [:path "foo"] "5"]
                 #{"foo"}]
                ["alias by node (3-arity)"
                 "aliasByNode(foo,5,30)"
                 [:aliasnode [:path "foo"] "5" "30"]
                 #{"foo"}]

                ["alias sub"
                 "aliasSub(foo,'foo','bar')"
                 [:aliassub [:path "foo"] "foo" "bar"]
                 #{"foo"}]

                ["alpha"
                 "alpha(foo,0.5)"
                 [:alpha [:path "foo"] "0.5"]
                 #{"foo"}]

                ["area between"
                 "areaBetween(foo,bar,baz)"
                 [:areabetween [:path "foo"]
                  [:path "bar"]
                  [:path "baz"]]
                 #{"foo" "bar" "baz"}]

                ["as percent (arity-1)"
                 "aspercent(foo)"
                 [:aspercent [:path "foo"]]
                 #{"foo"}]
                ["as percent (arity-2)"
                 "asPercent(foo,100)"
                 [:aspercent [:path "foo"] "100"]
                 #{"foo"}]
                ["as percent (arity-2 with expr)"
                 "asPercent(foo,alias(bar,'baz'))"
                 [:aspercent [:path "foo"] [:alias [:path "bar"] "baz"]]
                 #{"foo" "bar"}]

                ["average agove"
                 "averageAbove(foo,5.2)"
                 [:avgabove [:path "foo"] "5.2"]
                 #{"foo"}]

                ["average below"
                 "averageBelow(foo,5.2)"
                 [:avgbelow [:path "foo"] "5.2"]
                 #{"foo"}]

                ["group"
                 "group(foo,bar,baz,foo,qux)"
                 [:group [:path "foo"]
                  [:path "bar"]
                  [:path "baz"]
                  [:path "foo"]
                  [:path "qux"]]
                 #{"foo" "bar" "baz" "qux"}]

                ["scale"
                 "scale(foo,5)"
                 [:scale [:path "foo"] "5"]
                 #{"foo"}]

]]
    (doseq [[shortname query expected-tokens paths] inputs
            :let [tokens (parser/query->tokens query)]]
      (testing (str "tokens output for " shortname)
        (is (= expected-tokens tokens)))
      (testing (str "parsed paths for " shortname)
        (is (= paths (path/tokens->paths tokens)))))))
