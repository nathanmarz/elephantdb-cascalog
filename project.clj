(defproject elephantdb/elephantdb-cascalog "0.2.0"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :test-path "test/clj"
  :javac-options {:debug "true" :fork "true"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [elephantdb/elephantdb-cascading "0.2.0"]
                 [cascalog "1.8.4"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
