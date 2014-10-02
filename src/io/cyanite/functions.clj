(ns io.cyanite.functions)


(defmulti apply-ast (fn [arg & _] (some-> (vector? arg) first)))

(defmethod apply-ast :path
  [[_ path] series from to]
  (get series path))

(defmethod apply-ast :alias
  [[_ ast new-name] series from to]
  (-> (apply-ast ast series from to)
      (assoc-in [:series :name] new-name)))
