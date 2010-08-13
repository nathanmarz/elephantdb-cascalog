(ns elephantdb.cascalog.core
  (:import [elephantdb.cascading Common ElephantDBTap])
  (:import [elephantdb.hadoop ReplaceUpdater])
  (:import [elephantdb.cascalog ClojureUpdater])
  (:import [cascalog.ops IdentityBuffer])
  (:use [cascalog api])
  (:use [elephantdb.cascalog impl])
  (:require [cascalog [workflow :as w]])
  (:require [elephantdb [config :as c]]))

(defstruct ElephantArgs :persistence-options :tmp-dirs :updater :timeout-ms)

(def DEFAULT-ARGS (struct ElephantArgs {} nil (ReplaceUpdater.) nil))

(defn mk-clj-updater
  "Can be given either a var or a vector of var and args (for HOF)"
  [updater-spec]
  (ClojureUpdater. (w/fn-spec updater-spec)))

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
        (shardify [(.getNumShards spec)] !key :> !shard)
        (:sort !sort-key)
        ((IdentityBuffer.) !keyraw !valueraw :> !key !value)
        )))

(defn write-to-elephant! [tap pairs-sq]
  (let [sq (elephant<- tap pairs-sq)]
    (?- tap sq)
    ))
