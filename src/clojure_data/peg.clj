(ns clojure-data.peg
  (require [taoensso.timbre :refer [trace info]]
           [taoensso.timbre.profiling :refer [p]]))

;; (timbre/set-level! :warn)

(comment
  "This implementation closely follows [A Text Pattern-Matching Tool based on Parsing Expression Grammars](http://www.inf.puc-rio.br/~roberto/docs/peg.pdf)")

(defn literal [string]
  (map #(vector :char %) string))

(defn regexp [re]
  [[:regexp (re-pattern (str "\\A" (.toString re)))]])

(defn any [] [[:any]])

(defn- foldr [f coll]
  (reduce (fn [x y] (f y x)) (reverse coll)))

(defn- choose2 [lhs rhs]
  (concat
    [[:choice (+ (count lhs) 2)]]
    lhs
    [[:commit (+ (count rhs) 1)]]
    rhs))

(defn choice [& options]
  (foldr choose2 options))

(defn rep* [pattern]
  (concat
    [[:choice (+ (count pattern) 2)]]
    pattern
    [[:partial-commit (- (count pattern))]]))

(defn peek-not [pattern]
  (concat
    [[:choice (+ (count pattern) 2)]]
    pattern
    [[:fail-twice]]))

(defn peek-and [pattern]
  (concat
    [[:choice (+ (count pattern) 2)]]
    pattern
    [[:back-commit 2]]
    [[:fail]]))

(defn call [rule]
  [[:call rule]])

;; TODO: check for loops: symbolically check if pattern accepts the empty string
(defn rule [& patterns]
  (concat
    (mapcat
      #(condp instance? %
        String                  (literal %)
        clojure.lang.Keyword    (call %)
        java.util.regex.Pattern (regexp %)
        %)
      patterns)))

;; TODO: do tail call opt by replacing :call, :return seqs with :jump
(defn grammar [start rule-set]
  (let [{:keys [code symbol-table]}
        (reduce
          (fn [program [name pattern]]
            { :code
              (concat
                (:code program)
                pattern
                [[:return]])
              :symbol-table
              (assoc (:symbol-table program) name (count (:code program)))
            })
          { :code [[:call start] [:jump nil]] :symbol-table {} }
          rule-set)]
  (map #(if (= :call (first %))
            [:call (symbol-table (second %))] ;; TODO: throw here if not found
            %)
    ;; replace the [:jump nil] with the offset to the [:end] instruction at the end of the code
    (concat (assoc-in (vec code) [1 1] (dec (count code))) [[:end]]))))

;; NB: this is not noticeably faster with transients for the stack
(defn parse [code input]
  (loop [pc 0
         ic 0
         stack []
         captures []]
    (if (nil? pc) ;; fail
      (if (integer? (first stack))
          (recur nil ic (rest stack) captures)
          (if (empty? stack)
              "error"
              (recur (ffirst stack) (second (first stack)) (rest stack) (nth (first stack) 2))))
      (let [inst (nth code pc)]
        ;; (trace inst)
        (case (first inst)
          :char
                  (if (and (< ic (count input)) (= (second inst) (.charAt input ic)))
                      (recur (inc pc) (inc ic) stack captures)
                      (recur nil ic stack captures))
          :string
                  (if (and (< ic (count input)) (.startsWith input (second inst) ic))
                      (recur (inc pc) (+ ic (count (second inst))) stack captures)
                      (recur nil ic stack captures))
          :regexp (if (< ic (count input))
                      (if-let [match (re-find (second inst) (.substring input ic))]
                              (recur (inc pc) (+ ic (count match)) stack captures)
                              (recur nil ic stack captures))
                      (recur nil ic stack captures))
          :any    (if (< ic (count input))
                      (recur (inc pc) (inc ic) stack captures)
                      (recur nil ic stack captures))
          :jump     (recur (+ pc (second inst)) ic stack captures)
          :choice   (recur (inc pc) ic (cons [(+ pc (second inst)) ic captures] stack) captures)
          :call     (recur (second inst) ic (cons (inc pc) stack) captures)
          :return   (recur (first stack) ic (rest stack) captures)
          :commit   (recur (+ pc (second inst)) ic (rest stack) captures)
          :capture  (recur (inc pc) ic stack (cons [ic pc] captures))
          :fail     (recur nil ic stack captures)
          :end      "success"
          :partial-commit (recur (+ pc (second inst)) ic (cons [ (ffirst stack) ic captures ] (rest stack)) captures)
          :fail-twice   (recur nil ic (rest stack) captures)
          :back-commit  (recur (+ pc (second inst)) (second (first stack)) (rest stack) (nth (first stack) 2))
          ;; TODO: do testchar optimizations here
          )))))
