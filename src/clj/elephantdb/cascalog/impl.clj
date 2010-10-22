(ns elephantdb.cascalog.impl
  (:import [elephantdb.cascading Common ElephantDBTap ElephantDBTap$Args])
  (:import [elephantdb.persistence LocalPersistenceFactory])
  (:import [elephantdb Utils])
  (:import [java.util ArrayList HashMap])
  (:import [org.apache.hadoop.io BytesWritable])
  (:use [cascalog api])
  )

(defn- serializable-persistence-options [options]
  (HashMap. options))

(defn- serializable-list [l]
  (when l
    (ArrayList. l)))

(defn convert-clj-args [args]
  (let [ret (ElephantDBTap$Args.)]
    (set! (. ret persistenceOptions) (serializable-persistence-options (:persistence-options args)))
    (set! (. ret tmpDirs) (serializable-list (:tmp-dirs args)))
    (set! (. ret updater) (:updater args))
    (if-let [to (:timeout-ms args)]
      (set! (. ret timeoutMs) to))
    (set! (. ret deserializer) (:deserializer args))
    (set! (. ret version) (:version args))
    ret
    ))

(defmapop [shardify [#^Integer num-shards]]
  [k]
  (Utils/keyShard (Common/serializeElephantVal k) num-shards))

(defmapop [mk-sortable-key [#^LocalPersistenceFactory fact]]
  [k]
  (BytesWritable.
   (.getSortableKey
    (.getKeySorter fact)
    (Common/serializeElephantVal k))))
