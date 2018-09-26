(ns net.intelie.challenges.mem-event-store_test
  (:import [net.intelie.challenges InMemoryEventStore Event])
  (:require [stateful-check.core :refer [specification-correct?]]
            [clojure.test.check.generators :as gen]
            [clojure.test :refer [deftest is]]))

(def system-under-test (InMemoryEventStore.))

(def timestamps (take 1000 (range)))

(def insert-store-spec
  {:args (fn [state] [(gen/elements timestamps)])
   
   :command #(.insert system-under-test (Event. "type" %))
   
   :next-state (fn [state [time] _]
                 (update-in state ["type" time] conj (Event. "type" time)))})

(def query-store-spec
  {:args (fn [state] [(gen/elements timestamps)])

   :command #(.moveNext (.query system-under-test "type" % (inc %)))
   
   :postcondition (fn [prev-state _ [time] result]
                    (let [previous (get-in prev-state ["type" time])]
                      (= result (not (empty? previous)))))})

(def remove-store-spec
  {:requires (fn [state] (seq (get state "type")))

   :args (fn [state]
           [(gen/elements (keys (get state "type")))])
   
   :command #(let [iterator (.query system-under-test "type" % (inc %))]
               (while (.moveNext iterator) (.remove iterator)))
   
   :next-state (fn [state [time] _]
                 (update-in state ["type"] dissoc time))})

(def store-spec
  {:setup #(.removeAll system-under-test "type")
   
   :commands {:insert #'insert-store-spec
              :query #'query-store-spec
              :rem #'remove-store-spec}})

(deftest test-store-sequentially
  (is (specification-correct? store-spec)))

(deftest test-store-concurrently
  (is (specification-correct? store-spec {:gen {:threads 2}
                                          :run {:max-tries 100}})))

;
;; usage:
;; You need Leiningen build tool
;;
;; to run:
;; lein test
;
