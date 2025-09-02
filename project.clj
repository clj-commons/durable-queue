(defproject org.clj-commons/durable-queue
  (or (System/getenv "PROJECT_VERSION") "0.1.7")
  :description "a in-process task-queue that is backed by disk."
  :url "https://github.com/clj-commons/durable-queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases true}]]
  :dependencies [[com.taoensso/nippy "3.6.0"]
                 [org.clj-commons/primitive-math "1.0.1"]
                 [org.clj-commons/byte-streams "0.3.4"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]
                                  [criterium/criterium "0.4.6"]]}
             :1.10 {:dependencies [[org.clojure/clojure "1.10.3"]]}
             :1.11 {:dependencies [[org.clojure/clojure "1.11.4"]]}
             :1.12 {:dependencies [[org.clojure/clojure "1.12.2"]]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default #(not (some #{:benchmark :stress} (keys %)))
                   :benchmark :benchmark
                   :stress :stress}
  :codox {:writer codox-md.writer/write-docs
          :include [clj-commons.durable-queue]}
  :jvm-opts ^:replace ["-server" "-Xmx100m"])
