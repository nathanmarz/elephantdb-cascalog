(ns elephantdb.cascalog.integration-test
  (:use clojure.test)
  (:use [cascalog api testing])
  (:use [elephantdb.cascalog core])
  (:require [elephantdb [testing :as e]])
  (:import [org.apache.hadoop.io BytesWritable])
  (:import [elephantdb.persistence JavaBerkDB])
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
      (write-to-elephant!
       (elephant-tap tmp {:num-shards 4 :persistence-factory (JavaBerkDB.)} {})
       source)
      (e/with-single-service-handler [handler {"domain" tmp}]
        (e/check-domain "domain" handler data))
      (write-to-elephant!
       (elephant-tap tmp {:updater (mk-clj-updater #'merge-updater)})
       source2)
      (e/with-single-service-handler [handler {"domain" tmp}]
        (e/check-domain "domain" handler data3)
        ))))

