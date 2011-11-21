(ns elephantdb.cascalog.core
  (:use cascalog api
        elephantdb.cascalog
        elephantdb.impl)
  (:require [cascalog.workflow :as w]
            [elephantdb.config :as c])
  (:import [elephantdb Utils]
           [elephantdb.cascalog ClojureUpdater]
           [elephantdb.cascading ElephantDBTap]
           [elephantdb.hadoop ReplaceUpdater Common
            LongDeserializer StringDeserializer IntDeserializer]
           [org.apache.hadoop.conf Configuration]))

;; TODO: This shouldn't be a struct.
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
  "Accepts a var OR a vector of a var and arguments. If this occurs,
  the var will be applied to the other arguments before returning a
  function. For example, given:

  (defn make-adder [x]
      (fn [y] (+ x y)))

  Either of these are valid:

  (mk-clj-updater [#'make-adder 1])
  (mk-clj-updater #'inc)"
  [updater-spec]
  (ClojureUpdater. (w/fn-spec updater-spec)))

(defn long-deserializer
  "Deserializes long byte arrays."
  []
  (LongDeserializer.))

(defn int-deserializer
  "Deserializes long byte arrays."
  []
  (IntDeserializer.))

(defn string-deserializer
  "Deserializes string byte arrays."
  []
  (StringDeserializer.))

(defn elephant-tap
  [root & {:keys [args domain-spec]}]
  (let [args (convert-clj-args (merge DEFAULT-ARGS args))
        domain-spec (when domain-spec (c/convert-clj-domain-spec domain-spec))
        etap (ElephantDBTap. root domain-spec args)]
    (cascalog-tap etap
                  (fn [pairs]
                    [etap (elephant<- etap pairs)]))))

(defn reshard!
  [source-dir target-dir numshards]
  (let [fs (Utils/getFS source-dir (Configuration.))
        spec (c/read-domain-spec fs source-dir)
        new-spec (assoc spec :num-shards numshards)]
    (?- (elephant-tap target-dir :domain-spec new-spec)
        (elephant-tap source-dir))))
