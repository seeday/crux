(ns crux.memory
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io DataOutputStream]
           java.nio.ByteBuffer
           java.util.Comparator
           java.util.function.Supplier
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer
           org.agrona.io.ExpandableDirectBufferOutputStream
           crux.ByteUtils
           [clojure.lang Compiler DynamicClassLoader Reflector RT]
           [clojure.asm ClassWriter Opcodes Type]))

(defprotocol MemoryRegion
  (->on-heap ^bytes [this])

  (->off-heap
    ^org.agrona.MutableDirectBuffer [this]
    ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to])

  (off-heap? [this])

  (as-buffer ^org.agrona.MutableDirectBuffer [this])

  (^long capacity [this]))

(def ^:private ^:const default-chunk-size (* 128 1024))
(def ^:private ^:const large-buffer-size (quot default-chunk-size 4))

(def ^:private ^ThreadLocal chunk-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ByteBuffer/allocateDirect default-chunk-size)))))

(defn allocate-unpooled-buffer ^org.agrona.MutableDirectBuffer [^long size]
  (UnsafeBuffer. (ByteBuffer/allocateDirect size) 0 size))

(def ^:private ^:const alignment-round-mask 0xf)

(defn allocate-buffer ^org.agrona.MutableDirectBuffer [^long size]
  (let [chunk ^ByteBuffer (.get chunk-tl)
        offset (.position chunk)]
    (cond
      (> size large-buffer-size)
      (allocate-unpooled-buffer size)

      (> size (.remaining chunk))
      (do (.set chunk-tl (ByteBuffer/allocateDirect default-chunk-size))
          (recur size))

      :else
      ;; TODO: This limit is safer if the byte buffer itself is used,
      ;; but slower.
      (let [buffer (.limit (.slice chunk) size)
            new-aligned-offset (bit-and (+ offset size alignment-round-mask)
                                        (bit-not alignment-round-mask))]
        (.position chunk new-aligned-offset)
        (UnsafeBuffer. ^ByteBuffer buffer 0 size)))))

(defn copy-buffer
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from]
   (copy-buffer from (capacity from)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit]
   (copy-buffer from limit (allocate-buffer limit)))
  (^org.agrona.MutableDirectBuffer [^DirectBuffer from ^long limit ^MutableDirectBuffer to]
   (doto to
     (.putBytes 0 from 0 limit))))

(defn copy-to-unpooled-buffer ^org.agrona.MutableDirectBuffer [^DirectBuffer from]
  (copy-buffer from (capacity from) (allocate-unpooled-buffer (capacity from))))

(defn slice-buffer ^org.agrona.MutableDirectBuffer [^DirectBuffer buffer ^long offset ^long limit]
  (UnsafeBuffer. buffer offset limit))

(defn limit-buffer ^org.agrona.MutableDirectBuffer [^DirectBuffer buffer ^long limit]
  (slice-buffer buffer 0 limit))

(defn with-buffer-out
  (^org.agrona.DirectBuffer [b f]
   (with-buffer-out b f true))
  (^org.agrona.DirectBuffer [b f copy?]
   (with-buffer-out b f copy? 0))
  (^org.agrona.DirectBuffer [b f copy? ^long offset]
   (let [b-out (ExpandableDirectBufferOutputStream. (or b (ExpandableDirectByteBuffer.)) offset)]
     (with-open [out (DataOutputStream. b-out)]
       (f out))
     (cond-> (UnsafeBuffer. (.buffer b-out) 0 (+ (.position b-out) offset))
       copy? (copy-buffer)))))

(extend-protocol MemoryRegion
  (class (byte-array 0))
  (->on-heap [this]
    this)

  (->off-heap
    ([this]
     (let [b (allocate-buffer (alength ^bytes this))]
       (->off-heap this b)))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 ^bytes this))))

  (off-heap? [this]
    false)

  (as-buffer [this]
    (UnsafeBuffer. ^bytes this))

  (capacity [this]
    (alength ^bytes this))

  DirectBuffer
  (->on-heap [this]
    (if (and (.byteArray this)
             (= (.capacity this)
                (alength (.byteArray this))))
      (.byteArray this)
      (let [bytes (byte-array (.capacity this))]
        (.getBytes this 0 bytes)
        bytes)))

  (->off-heap
    ([this]
     (if (off-heap? this)
       this
       (->off-heap this (allocate-buffer (.capacity this)))))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 this 0 (.capacity this)))))

  (off-heap? [this]
    (or (some-> (.byteBuffer this) (.isDirect))
        (and (nil? (.byteArray this))
             (nil? (.byteBuffer this)))))

  (as-buffer [this]
    this)

  (capacity [this]
    (.capacity this))

  ByteBuffer
  (->on-heap [this]
    (if (and (.hasArray this)
             (= (.remaining this)
                (alength (.array this))))
      (.array this)
      (doto (byte-array (.remaining this))
        (->> (.get this)))))

  (->off-heap
    ([this]
     (if (.isDirect this)
       (as-buffer this)
       (->off-heap this (allocate-buffer (.remaining this)))))

    ([this ^MutableDirectBuffer to]
     (doto to
       (.putBytes 0 this (.position this) (.remaining this)))))

  (off-heap? [this]
    (.isDirect this))

  (as-buffer [this]
    (UnsafeBuffer. this (.position this) (.remaining this)))

  (capacity [this]
    (.remaining this)))

(defn ensure-off-heap ^org.agrona.DirectBuffer [b ^MutableDirectBuffer tmp]
  (if (off-heap? b)
    b
    (UnsafeBuffer. (->off-heap b tmp) 0 (capacity b))))

(defn on-heap-buffer ^org.agrona.DirectBuffer [^bytes b]
  (UnsafeBuffer. b))

(defn buffer->hex ^String [^DirectBuffer b]
  (some-> b (ByteUtils/bufferToHex)))

(defn hex->buffer
  (^org.agrona.DirectBuffer [^String b]
   (hex->buffer b (ExpandableDirectByteBuffer.)))
  (^org.agrona.DirectBuffer [^String b ^MutableDirectBuffer to]
   (some-> b (ByteUtils/hexToBuffer to))))

(defn compare-buffers
  (^long [^DirectBuffer a ^DirectBuffer b]
   (ByteUtils/compareBuffers a b))
  (^long [^DirectBuffer a ^DirectBuffer b ^long max-length]
   (ByteUtils/compareBuffers a b max-length)))

(def ^java.util.Comparator buffer-comparator
  ByteUtils/UNSIGNED_BUFFER_COMPARATOR)

(defn buffers=?
  ([^DirectBuffer a ^DirectBuffer b]
   (ByteUtils/equalBuffers a b))
  ([^DirectBuffer a ^DirectBuffer b ^long max-length]
   (ByteUtils/equalBuffers a b max-length)))

(defn inc-unsigned-buffer!
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer buffer]
   (inc-unsigned-buffer! buffer (.capacity buffer)))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer buffer ^long prefix-length]
   (loop [idx (dec (int prefix-length))]
     (when-not (neg? idx)
       (let [b (Byte/toUnsignedInt (.getByte buffer idx))]
         (if (= 0xff b)
           (do (.putByte buffer idx (byte 0))
               (recur (dec idx)))
           (doto buffer
             (.putByte idx (unchecked-byte (inc b))))))))))

;; Value Objects, normal on heap classes with mutable public fields.

(defmacro define-class [fqn bs]
  (when-not *compile-files*
    `(.defineClass ^DynamicClassLoader (RT/makeClassLoader) (str ~fqn) ~bs "")))

(defn- define-value-object [fqn fields]
  (let [cw (ClassWriter. ClassWriter/COMPUTE_MAXS)
        cw (doto cw
             (.visit 52 ;; Opcodes/V1_8 ;; NOTE: Field does not exist in Clojure 1.9.
                     (bit-or Opcodes/ACC_PUBLIC
                             Opcodes/ACC_FINAL)
                     (str/replace (str fqn) "." "/")
                     nil
                     (.getInternalName (Type/getType Object))
                     (make-array String 0))
             (-> (.visitMethod 1 "<init>" "()V" nil nil)
                 (doto (.visitCode)
                   (.visitVarInsn Opcodes/ALOAD 0)
                   (.visitMethodInsn Opcodes/INVOKESPECIAL (.getInternalName (Type/getType Object)) "<init>" "()V")
                   (.visitInsn Opcodes/RETURN)
                   (.visitMaxs 0 0)
                   (.visitEnd))))]

    (doseq [f fields
            :let [tag (some-> f meta :tag)
                  f (Compiler/munge (str f))
                  type (case (str tag)
                         "boolean"
                         Type/BOOLEAN_TYPE
                         "booleans"
                         (Type/getType "[Z")

                         "byte"
                         Type/BYTE_TYPE
                         "bytes"
                         (Type/getType "[B")

                         "char"
                         Type/CHAR_TYPE
                         "chars"
                         (Type/getType "[C")

                         "short"
                         Type/SHORT_TYPE
                         "shorts"
                         (Type/getType "[S")

                         "int"
                         Type/INT_TYPE
                         "ints"
                         (Type/getType "[I")

                         "long"
                         Type/LONG_TYPE
                         "longs"
                         (Type/getType "[J")

                         "float"
                         Type/FLOAT_TYPE
                         "floats"
                         (Type/getType "[F")

                         "double"
                         Type/DOUBLE_TYPE
                         "doubles"
                         (Type/getType "[D")

                         "objects"
                         (Type/getType (str "[" (Type/getDescriptor Object)))

                         (cond
                           (string? tag)
                           (Type/getType (Class/forName (str tag)))

                           (symbol? tag)
                           (Type/getType ^Class (resolve tag))

                           :else
                           (Type/getType Object)))]]
      (.visitEnd (.visitField cw Opcodes/ACC_PUBLIC (str f) (.getDescriptor type) nil nil)))
    (let [bs (.toByteArray (doto cw
                             (.visitEnd)))]
      (let [f (io/file (str *compile-path* "/" (str/replace fqn "." "/") ".class"))]
        (io/make-parents f)
        (io/copy bs f))
      (define-class fqn bs))))

(defmacro defvo [name fields]
  (let [fqn (if-not (namespace name)
              (symbol (str (ns-name *ns*)
                           "."
                           name))
              name)
        vo-sym (gensym "vo")]
    (define-value-object fqn fields)
    `(do (import ~fqn)
         (defn ~(symbol (str "->" name)) ~(with-meta (mapv #(with-meta % nil) fields)
                                            {:tag fqn})
           (let [~vo-sym (~(symbol (str name ".")))]
             ~@(for [[k v] (zipmap (map keyword fields) fields)]
                 `(set! (~(symbol (str "." (clojure.core/name k))) ~vo-sym) ~v))
             ~vo-sym))
         ~fqn)))
