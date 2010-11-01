(ns elephantdb.cascalog.core
  (:import [elephantdb.cascading Common ElephantDBTap
            LongDeserializer StringDeserializer IntDeserializer])
  (:import [elephantdb.hadoop ReplaceUpdater])
  (:import [elephantdb.cascalog ClojureUpdater])
  (:import [cascalog.ops IdentityBuffer])
  (:import [org.apache.hadoop.conf Configuration])
  (:import [elephantdb Utils])
  (:use [cascalog api])
  (:use [elephantdb.cascalog impl])
  (:require [cascalog [workflow :as w]])
  (:require [elephantdb [config :as c]]))

(defstruct ElephantArgs
  :persistence-options
  :tmp-dirs
  :updater
  :timeout-ms
  :deserializer
  :version)

(def DEFAULT-ARGS
     (struct ElephantArgs
             {}
             nil
             (ReplaceUpdater.)
             nil
             nil
             nil))

(defn mk-clj-updater
  "Can be given either a var or a vector of var and args (for HOF)"
  [updater-spec]
  (ClojureUpdater. (w/fn-spec updater-spec)))

(defn long-deserializer []
  (LongDeserializer.))

(defn int-deserializer []
  (IntDeserializer.))

(defn string-deserializer []
  (StringDeserializer.))

(defn elephant-tap
  ([root]
     (elephant-tap root nil))
  ([root args]
     (elephant-tap root nil args))
  ([root domain-spec args]
     (let [args (convert-clj-args (merge DEFAULT-ARGS args))
           domain-spec (when domain-spec
                         (c/convert-clj-domain-spec domain-spec))]
       (ElephantDBTap. root domain-spec args)
       )))

(defn elephant<- [elephant-tap pairs-sq]
  (let [spec (.getSpec elephant-tap)]
    (<- [!shard !key !value]
        (pairs-sq !keyraw !valueraw)
        (mk-sortable-key [(.getLPFactory spec)] !keyraw :> !sort-key)
        (shardify [(.getNumShards spec)] !keyraw :> !shard)
        (:sort !sort-key)
        ((IdentityBuffer.) !keyraw !valueraw :> !key !value)
        )))

(defn write-to-elephant! [tap pairs-sq]
  (let [sq (elephant<- tap pairs-sq)]
    (?- tap sq)
    ))

(defn reshard! [source-dir target-dir numshards]
  (let [fs (Utils/getFS source-dir (Configuration.))
        spec (c/read-domain-spec fs source-dir)
        new-spec (assoc spec :num-shards numshards)]
    (write-to-elephant!
     (elephant-tap target-dir new-spec {})
     (elephant-tap source-dir))
    ))
