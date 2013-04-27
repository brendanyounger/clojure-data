(ns clojure-data.peg)

(comment
  "This implementation closely follows [A Text Pattern-Matching Tool based on Parsing Expression Grammars](http://www.inf.puc-rio.br/~roberto/docs/peg.pdf)")

(defn literal [string]
  (map #(vector :char %) string))

(defn regexp [re]
  [[:regexp (re-pattern (str "\\A" (.toString re)))]])

(defn any [] [:any])

(defn choice [lhs rhs]
  (concat
    [[:choice (+ (count lhs) 2)]]
    lhs
    [[:commit (+ (count rhs) 1)]]
    rhs))

(defn rep* [pattern]
  (concat
    [[:choice (+ (count pattern) 2)]]
    pattern
    [[:commit (- (inc (count pattern)))]]))

(defn peek-not [pattern]
  (concat
    [[:choice (+ (count pattern) 3)]]
    pattern
    [[:commit 1]]
    [[:fail]]))

(defn peek-and [pattern]
  (concat
    [[:choice (+ (count pattern) 4)]]
    [[:choice (+ (count pattern) 1)]]
    pattern
    [[:commit 1]]
    [[:fail]]))

(defn call [rule]
  [[:call rule]])

;; TODO: do tail call opt by replacing :call, :return seqs with :jump
;; TODO: check for loops: symbolically check if pattern accepts the empty string
(defn grammar [start rule-set]
  (let [program
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
    ;; replace the [:jump nil] with the offset to the [:end] instruction at the end of the code
  { :code (concat (assoc-in (vec (:code program)) [1 1] (dec (count (:code program)))) [[:end]])
    :symbol-table (:symbol-table program) }))

(defn empty-state [] { :pc 0 :ic 0 :stack [] :captures nil })

(defn parse [{ pc :pc ic :ic stack :stack captures :captures }
             { code :code symbol-table :symbol-table :as program}
             input]
  (let [next-state
  (if (nil? pc) ;; fail
      (if (integer? (first stack))
          { :pc nil :ic ic :stack (rest stack) :captures captures }
          (if (empty? stack)
              "error"
              { :pc (ffirst stack) :ic (second (first stack)) :stack (rest stack) :captures (nth (first stack) 2) }))
      (let [inst (nth code pc)]
        (println inst)
        (case (first inst)
          ;; TODO: guard against going past bounds of string here...
          :char (if (= (second inst) (.charAt input ic))
                    { :pc (inc pc) :ic (inc ic) :stack stack :captures captures }
                    { :pc nil :ic ic :stack stack :captures captures })
          :regexp (if (re-find (second inst) (.substring input ic))
                      { :pc (inc pc) :ic (+ ic (count (re-find (second inst) (.substring input ic)))) :stack stack :captures captures }
                      { :pc nil :ic ic :stack stack :captures captures })
          :jump { :pc (+ pc (second inst)) :ic ic :stack stack :captures captures }
          :choice { :pc (inc pc) :ic ic :stack (cons [(+ pc (second inst)) ic captures] stack) :captures captures }
          :call { :pc (symbol-table (second inst)) :ic ic :stack (cons (inc pc) stack) :captures captures }
          :return { :pc (first stack) :ic ic :stack (rest stack) :captures captures }
          :commit { :pc (+ pc (second inst)) :ic ic :stack (rest stack) :captures captures }
          :capture { :pc (inc pc) :ic ic :stack stack :captures (cons [ic pc] captures) }
          :fail { :pc nil :ic ic :stack stack :captures captures }
          :end "success")))
  ]
  (println next-state)
  (if (string? next-state) next-state (recur next-state program input))))

(def g (grammar :S { :S (concat (choice (call :B) (regexp #"[^()]"))) :B (concat (literal "(") (call :S) (literal ")")) }))
