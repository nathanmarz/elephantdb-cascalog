(ns elephantdb.cascalog.integration-test
  (:use clojure.test)
  (:use [cascalog api testing])
  (:use [elephantdb.cascalog core])
  (:require [elephantdb [testing :as e] [config :as config]])
  (:require [cascalog [ops :as c]])
  (:import [org.apache.hadoop.io BytesWritable])
  (:import [elephantdb.persistence JavaBerkDB])
  (:import [elephantdb Utils])
  )

(defn mk-writable-pairs [pairs]
  (map (fn [[k v]] [(BytesWritable. k) (BytesWritable. v)]) pairs))

(defn merge-updater [lp k v]
  (let [ov (.get lp k)
        nv (if ov
             (byte-array (concat (seq ov) (seq v)))
             v)]
    (.add lp k nv)
    ))

(e/deffstest test-all [fs tmp]
  (let [data [[(e/barr 0) (e/barr 1)]
              [(e/barr 1) (e/barr 2 2)]
              [(e/barr 3) (e/barr 3 3 4)]
              [(e/barr 1 1) (e/barr 2 2)]
              [(e/barr 2 2 2) (e/barr 3 3 3)]
              ]
        data2 [[(e/barr 0) (e/barr 10)]
               [(e/barr 3) (e/barr 3)]
               [(e/barr 10) (e/barr 10)]
               ]
        data3 [[(e/barr 0) (e/barr 1 10)]
               [(e/barr 1) (e/barr 2 2)]
               [(e/barr 3) (e/barr 3 3 4 3)]
               [(e/barr 1 1) (e/barr 2 2)]
               [(e/barr 2 2 2) (e/barr 3 3 3)]
               [(e/barr 10) (e/barr 10)]
               ]]
    (with-tmp-sources [source (mk-writable-pairs data)
                       source2 (mk-writable-pairs data2)]
      (?-
       (elephant-tap tmp {:num-shards 4 :persistence-factory (JavaBerkDB.)} {})
       source)
      (e/with-single-service-handler [handler {"domain" tmp}]
        (e/check-domain "domain" handler data))
      (?-
       (elephant-tap tmp {:updater (mk-clj-updater #'merge-updater)})
       source2)
      (e/with-single-service-handler [handler {"domain" tmp}]
        (e/check-domain "domain" handler data3)
        ))))

(defn test-to-int [bw]
  (int (first (.get bw))))

(deftest test-source
  (let [pairs [[(e/barr 0) (e/barr 1)]
               [(e/barr 1) (e/barr 2)]
               [(e/barr 2) (e/barr 3)]
               [(e/barr 3) (e/barr 0)]
               [(e/barr 4) (e/barr 0)]
               [(e/barr 5) (e/barr 1)]
               [(e/barr 6) (e/barr 3)]
               [(e/barr 7) (e/barr 9)]
               [(e/barr 8) (e/barr 99)]
               [(e/barr 9) (e/barr 4)]
               [(e/barr 10) (e/barr 3)]
               ]]
    (e/with-sharded-domain [dpath
                            {:num-shards 3
                             :persistence-factory (JavaBerkDB.)}
                            pairs]
      (test?<- [[1 2] [2 1] [3 3] [0 2] [9 1] [99 1] [4 1]]
               [?intval ?count]
               ((elephant-tap dpath) _ ?value)
               (test-to-int ?value :> ?intval)
               (c/count ?count))
      (test?<- [[0 1] [1 1] [2 1] [3 1] [4 1] [5 1] [6 1] [7 1] [8 1] [9 1] [10 1]]
               [?intval ?count]
               ((elephant-tap dpath) ?key _)
               (test-to-int ?key :> ?intval)
               (c/count ?count))
      )))

;; TODO: test read specific version using a deserializer

(deftest test-deserializer
  (let [pairs [[(Utils/serializeString "aaa") (e/barr 1)]]]
    (e/with-sharded-domain [dpath
                            {:num-shards 3
                             :persistence-factory (JavaBerkDB.)}
                            pairs]
      (test?<- [["aaa" 1]]
               [?key ?intval]
               ((elephant-tap dpath {:deserializer (string-deserializer)})
                ?key ?value)
               (test-to-int ?value :> ?intval))
      )))

(e/deffstest test-reshard [fs tmpout1 tmpout2]
  (let [pairs [[(e/barr 0) (e/barr 1)]
               [(e/barr 1) (e/barr 2)]
               [(e/barr 2) (e/barr 3)]
               [(e/barr 3) (e/barr 0)]
               [(e/barr 4) (e/barr 0)]
               [(e/barr 5) (e/barr 1)]
               ]]
    (e/with-sharded-domain [dpath
                            {:num-shards 3
                             :persistence-factory (JavaBerkDB.)}
                            pairs]
      (reshard! dpath tmpout1 1)
      (is (= 1 (:num-shards (config/read-domain-spec fs tmpout1))))
      (e/with-single-service-handler [handler {"domain" tmpout1}]
        (e/check-domain "domain" handler pairs))
      (reshard! dpath tmpout2 2)
      (is (= 1 (:num-shards (config/read-domain-spec fs tmpout1))))
      (e/with-single-service-handler [handler {"domain" tmpout2}]
        (e/check-domain "domain" handler pairs))
      )))
