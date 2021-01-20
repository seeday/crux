(ns crux.redis
  (:require  [clojure.string :as str]
             [crux.codec :as c]
             [crux.db :as db]
             [crux.document-store :as ds]
             [crux.io :as cio]
             [crux.system :as sys]
             [crux.tx :as tx]
             [clojure.spec.alpha :as s]
             [taoensso.carmine :as car]
             [taoensso.nippy :as nippy])
  (:import [java.io Closeable]
           [java.util Date Map]
           [java.net URI]
           [java.nio ByteBuffer]
           [io.lettuce.core RedisClient AbstractRedisClient KeyValue XReadArgs XReadArgs$StreamOffset Range Limit StreamMessage]
           [io.lettuce.core.codec RedisCodec]
           [io.lettuce.core.api.async RedisAsyncCommands]
           [io.lettuce.core.cluster RedisClusterClient]))

(defn ^"[B" bb->bytes [^ByteBuffer bb]
  (let [bytes (byte-array (.remaining bb))]
    (.get bb bytes)
    bytes))

(defn ^ByteBuffer bytes->bb [^bytes b]
  (ByteBuffer/wrap b))

(defn nippy-codec
  ([] (nippy-codec nil nil))
  ([opts] (nippy-codec opts opts))
  ([freeze-opts thaw-opts]
   (proxy [RedisCodec] []
     (decodeKey [bb] (String. (bb->bytes bb)))
     (decodeValue [bb]
       (nippy/thaw (bb->bytes bb) thaw-opts))
     (encodeKey [k] (bytes->bb (.getBytes k)))
     (encodeValue [v]
       (bytes->bb (nippy/freeze v freeze-opts))))))

(defn- decompose-redis-id [id] (map #(Long/parseUnsignedLong %) (str/split id #"-" 2)))

(defn- txid->redisid [id]
  (let [seqn (bit-and id 0xffff)
        time (unsigned-bit-shift-right id 16)]
    (format "%d-%d" time seqn)))

(defn- redisid->txid
  ([id]
   (let [[time seqn] (decompose-redis-id id)]
     (redisid->txid time seqn)))
  ([time seqn]
   (assert (<= seqn 0xffff))
   (bit-or (bit-shift-left time 16) seqn)))

(defrecord RedisTxLog [^AbstractRedisClient client ^RedisAsyncCommands cmds ^Closeable tx-consumer]
  db/TxLog
  (submit-tx [_ tx-events]
    ;; (println "tx-events" tx-events)
    (let [res @(.xadd cmds "txs" {"" tx-events})
          [time seqn] (decompose-redis-id res)
          tx-data {:crux.tx/tx-id (long (redisid->txid time seqn))
                   :crux.tx/tx-time (Date. (long time))}]
      (delay tx-data)))

  (open-tx-log [_ after-tx-id]
    (cio/->cursor #(do)
                  (let [res @(.xread cmds (-> (XReadArgs.)
                                              (.block 1000))
                                    (into-array XReadArgs$StreamOffset
                                                [(XReadArgs$StreamOffset/from "txs" (txid->redisid after-tx-id))]))
                        mapped (map (fn [^StreamMessage r]
                                      {:crux.tx/tx-id (redisid->txid (.getId r))
                                       :crux.tx/tx-time (Date. (long (first (decompose-redis-id (.getId r)))))
                                       :crux.tx.event/tx-events (get (.getBody r) "")})
                                    res)]
                    mapped)))

  (latest-submitted-tx [_]
    (let [^StreamMessage resp (first @(.xrevrange cmds "txs"
                                                  (Range/create "-" "+")
                                                  (Limit/from 1)))
          [time seqn] (decompose-redis-id (.getId resp))]
      {:crux.tx/tx-id (redisid->txid time seqn)
       :crux.tx/tx-time (Date. (long time))}))

  Closeable
  (close [_]
    (cio/try-close tx-consumer)
    (.shutdown client)))

(defn- create-client [cluster? uri]
  (let [^AbstractRedisClient client (if cluster? (RedisClusterClient/create ^String uri)
                                        (RedisClient/create ^String uri))
        ^RedisAsyncCommands cmds (-> (.connect client ^RedisCodec (nippy-codec {:compressor nippy/snappy-compressor
                                                                                :no-header? true
                                                                                :encryptor nil}))
                                     (.async))]
    [client cmds]))

(defn ->ingest-only-tx-log {::sys/args {:uri {:doc "a redis URI"
                                              :required? true
                                              :default "redis://localhost:6379"}
                                        :cluster? {:required? true
                                                   :default false}}}
  [{:keys [uri cluster?]}]
  (let [[client cmds] (create-client cluster? uri)]
    (map->RedisTxLog {:client client :cmds cmds})))

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

(defrecord RedisDocumentStore [^AbstractRedisClient client ^RedisAsyncCommands cmds]
  db/DocumentStore
  (submit-docs [_ id-and-docs]
    @(.mset cmds (reduce-kv (fn [m k v] (assoc m (str k) v)) {} id-and-docs)))

  (-fetch-docs [_ ids]
    (if (= 0 (count ids))
      {}
      (reduce (fn [acc ^KeyValue m]
                (assoc acc (c/hex->id-buffer (.getKey m)) (.getValue m)))
              {}
              (filter #(.hasValue ^KeyValue %)
                      @(.mget cmds (into-array String (map (comp str c/new-id) ids)))))))
  Closeable
  (close [_]
    (.shutdown client)))

(defn ->document-store {::sys/args {:cluster? {:doc "use clustered redis mode?"
                                               :required? false
                                               :default false}
                                    :uri {:doc "a redis connection URI"
                                          :required? true
                                          :default "redis://localhost:6379"}}}
  [{:keys [cluster? uri]}]
  (let [[client cmds] (create-client cluster? uri)]
    (->RedisDocumentStore client cmds)))
