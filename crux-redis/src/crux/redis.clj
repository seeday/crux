(ns crux.redis
  (:require  [clojure.string :as str]
             [crux.codec :as c]
             [crux.db :as db]
             [crux.document-store :as ds]
             [crux.io :as cio]
             [crux.system :as sys]
             [crux.tx :as tx]
             [taoensso.carmine :as car])
  (:import [java.io Closeable]
           [java.util Date]))

(defn- decompose-redis-id [id] (map #(Long/parseUnsignedLong %) (str/split id #"-" 2)))

(defn- txid->redisid [id]
  (let [seqn (bit-and id 0xffff)
        time (unsigned-bit-shift-right id 16)]
    (format "%d-%d" time seqn)))

(defn- redisid->txid [id]
  (let [[time seqn] (decompose-redis-id id)]
    (assert (<= seqn 0xffff))
    (bit-or (bit-shift-left time 16) seqn)))

(defrecord RedisTxLog [conn]
  db/TxLog
  (submit-tx [this tx-events]
    ;; (println "tx-events" tx-events)
    (let [res (car/wcar conn (car/xadd "txs" "*" "tx" tx-events))
          id (redisid->txid res)
          tx-data {:crux.tx/tx-id (long id)
                   :crux.tx/tx-time (-> res
                                    (decompose-redis-id)
                                    (first)
                                    (Date.))}]
      (delay tx-data)))

  (open-tx-log [this after-tx-id]
    ;; TODO: probably do xread in batches of 100 or so
    ;; take a look at the kafka tx-log and use lazy-seq
    (cio/->cursor #(do)
                  (let [res (car/wcar conn (car/xread :streams "txs" (txid->redisid after-tx-id)))
                        mapped (map (fn [r]
                                      {:crux.tx/tx-id (redisid->txid (first r))
                                       :crux.tx/tx-time (Date. (first (decompose-redis-id (first r))))
                                       :crux.tx.event/tx-events (second (second r))})
                                    (-> res (first) (second)))]
                    ;; (println "snagging txs" mapped)
                    mapped)))

  (latest-submitted-tx [this]
    (let [resp (car/wcar conn (car/xrevrange "txs" "+" "-" :count 1))]
      {:crux.tx/tx-id (-> resp (ffirst) (redisid->txid))
       :crux.tx/tx-time (-> resp
                            (ffirst)
                            (decompose-redis-id)
                            (first)
                            (Date.))}))

  Closeable
  (close [_]))

(defn ->ingest-only-tx-log {::sys/args {:connection-spec {:doc "a carmine connection map with optional pool settings"
                                              :required? true}}}
  [{:keys [connection-spec] :as opts}]
  (->RedisTxLog connection-spec))

(defn ->tx-log {::sys/deps (merge (::sys/deps (meta #'tx/->polling-tx-consumer))
                                  (::sys/deps (meta #'->ingest-only-tx-log)))
                ::sys/args (merge (::sys/args (meta #'tx/->polling-tx-consumer))
                                  (::sys/args (meta #'->ingest-only-tx-log)))}
  [opts]
  (let [tx-log (->ingest-only-tx-log opts)]
    (-> tx-log
        (assoc :tx-consumer (tx/->polling-tx-consumer opts
                                                      (fn [after-tx-id]
                                                        (db/open-tx-log tx-log (or after-tx-id 0))))))))

(defrecord RedisDocumentStore [conn]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (car/wcar conn
              (doseq [[id doc] id-and-docs
                      :let [id (str id)]]
                ;; TODO: find out why the test is written to not actually evict things
                ;; (if (c/evicted-doc? doc)
                ;;   (car/del id)
                ;;   (car/set id doc))
                (car/set id doc))))

  (-fetch-docs [this ids]
    (if (= 0 (count ids))
      {}
      (into {}
            (filter (comp some? val)
                    (zipmap ids (car/wcar conn (apply car/mget (map (comp str c/new-id) ids)))))))))

(defn ->document-store {::sys/args {:connection-spec {:doc "a carmine connection map with optional pool"
                                                      :required? true}}}
  [{:keys [connection-spec] :as opts}]
  (->RedisDocumentStore connection-spec)
  )
