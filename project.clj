(defproject factual/durable-queue "0.1.6"
  :description "a in-process task-queue that is backed by disk."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.taoensso/nippy "2.8.0"]
                 [primitive-math "0.1.4"]
                 [byte-streams "0.2.0"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.5.1"]
                                  [criterium "0.4.3"]
                                  [codox-md "0.2.0" :exclusions [org.clojure/clojure]]]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default #(not (some #{:benchmark :stress} (keys %)))
                   :benchmark :benchmark
                   :stress :stress}
  :plugins [[codox "0.6.6"]]
  :codox {:writer codox-md.writer/write-docs
          :include [durable-queue]}
  :jvm-opts ^:replace ["-server" "-Xmx100m"])
