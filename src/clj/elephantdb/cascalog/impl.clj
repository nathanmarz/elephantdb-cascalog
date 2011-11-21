(ns elephantdb.cascalog.impl
  (:import [elephantdb.cascading Common ElephantDBTap ElephantDBTap$Args])
  (:import [elephantdb.persistence LocalPersistenceFactory])
  (:import [elephantdb Utils])
  (:import [java.util ArrayList HashMap])
  (:import [cascalog.ops IdentityBuffer])
  (:import [org.apache.hadoop.io BytesWritable])
  (:use [cascalog api]))

(defn- serializable-persistence-options [options]
  (HashMap. options))

(defn- serializable-list [l]
  (when l (ArrayList. l)))

(defn convert-clj-args [args]
  (let [ret (ElephantDBTap$Args.)]
    (set! (.persistenceOptions ret) (serializable-persistence-options (:persistence-options args)))
    (set! (.tmpDirs ret) (serializable-list (:tmp-dirs args)))
    (set! (.updater ret) (:updater args))
    (if-let [to (:timeout-ms args)]
      (set! (.timeoutMs ret) to))
    (set! (.deserializer ret) (:deserializer args))
    (set! (.version ret) (:version args))
    ret))

(defmapop [shardify [^Integer num-shards]]
  [k]
  (Utils/keyShard (Common/serializeElephantVal k)
                  num-shards))

(defmapop [mk-sortable-key [#^LocalPersistenceFactory fact]]
  [k]
  (BytesWritable.
   (.getSortableKey
    (.getKeySorter fact)
    (Common/serializeElephantVal k))))

(defn elephant<- [elephant-tap pairs-sq]
  (let [spec (.getSpec elephant-tap)]
    (<- [!shard !key !value]
        (pairs-sq !keyraw !valueraw)
        (mk-sortable-key [(.getLPFactory spec)] !keyraw :> !sort-key)
        (shardify [(.getNumShards spec)] !keyraw :> !shard)
        (:sort !sort-key)
        ((IdentityBuffer.) !keyraw !valueraw :> !key !value)
        )))
