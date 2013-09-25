(defproject durable-queue "0.1.0-SNAPSHOT"
  :description "a in-process task-queue that is backed by disk."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/nippy "2.1.0"]
                 [byte-streams "0.1.6-SNAPSHOT"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]
                                  [criterium "0.4.2"]
                                  [codox-md "0.2.0" :exclusions [org.clojure/clojure]]]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default (complement :benchmark)
                   :benchmark :benchmark}
  :plugins [[codox "0.6.6"]]
  :codox {:writer codox-md.writer/write-docs
          :include [durable-queue]}
  :jvm-opts ^:replace ["-server"])
