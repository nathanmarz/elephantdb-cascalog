(defproject elephantdb/elephantdb-cascalog "0.1.0"
  :source-path "src/clj"
  :test-path "test/clj"
  :java-source-path "src/jvm"
  :java-fork "true"
  :javac-debug "true"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [elephantdb/elephantdb-cascading "0.1.0"]
                 [cascalog "1.8.2"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
                     [swank-clojure "1.4.0-SNAPSHOT"]])
