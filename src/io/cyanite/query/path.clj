(ns io.cyanite.query.path)

(defn extract-paths
  "Extract paths from an AST"
  [tokens]
  (if (sequential? tokens)
    (let [[opcode & args] tokens]
      (if (= opcode :path)
        args
        (mapcat extract-paths args)))
    []))

(defn tokens->paths
  "Unique list of paths to get for an AST"
  [ast]
  (set (extract-paths ast)))
