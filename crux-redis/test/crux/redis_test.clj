(ns crux.redis-test
  (:require  [clojure.test :as t]
             [crux.doc-store-test :as dst]
             [crux.db :as db]
             [crux.redis :as r]
             [crux.codec :as c]
             [crux.system :as sys]
             [criterium.core :as bench]))

(t/use-fixtures :each
  (fn [f]
    (with-open [sys (-> (sys/prep-system {::r/document-store
                                          {:connection-spec {:pool {} :spec {:uri "redis://localhost:6379"}}}})
                        (sys/start-system))]
      (binding [dst/*doc-store* (::r/document-store sys)]
        (f)))))

(t/deftest _)

(t/deftest test-tx-log-utils
  (t/is (= [1234 567] (apply #'r/decompose-redis-id ["1234-567"])))
  (t/is (= "18-54919" (apply #'r/txid->redisid [1234567]))))

(t/deftest test-tx-log
  (let [sys (-> (sys/prep-system {::r/tx-log
                                  {:connection-spec {:pool {} :spec {:uri "redis://localhost:6379"}}}})
                (sys/start-system))
        tx-log (::r/tx-log sys)]
    (let [txid @(db/submit-tx tx-log [[:crux.tx/put {:crux.db/id :yeeee :hello :world}]])]
      (t/is (= txid (db/latest-submitted-tx tx-log)))
      (t/is (some #(= (:crux.tx/tx-id txid) (:crux.tx/tx-id %))
                  (iterator-seq (db/open-tx-log tx-log 0)))))))

(t/deftest bench-inserts
  (let [sys (-> (sys/prep-system {::r/document-store
                                  {:connection-spec {:pool {} :spec {:uri "redis://localhost:6379"}}}})
                (sys/start-system))
        doc-store (::r/document-store sys)
        data {:crux.db/id :hello :hello :world :how :are :you :today}
        data-key (c/new-id data)]
    (t/is (= 4 (count data)))
    (bench/with-progress-reporting
      (bench/quick-bench (db/submit-docs doc-store {data-key data})))))

(defn test-ns-hook []
  (test-tx-log-utils)
  (test-tx-log)
  (dst/test-doc-store (find-ns 'crux.redis-test))
  ;; (bench-inserts)
  )
