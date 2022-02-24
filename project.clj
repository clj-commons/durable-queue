(defproject org.clj-commons/durable-queue
  (or (System/getenv "PROJECT_VERSION") "0.1.6")
  :description "a in-process task-queue that is backed by disk."
  :url "https://github.com/clj-commons/durable-queue"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases true}]]
  :dependencies [[com.taoensso/nippy "2.8.0"]
                 [primitive-math "0.1.4"]
                 [byte-streams "0.2.2"]]
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.0"]
                                  [criterium "0.4.6"]
]}}
  :global-vars {*warn-on-reflection* true}
  :test-selectors {:default #(not (some #{:benchmark :stress} (keys %)))
                   :benchmark :benchmark
                   :stress :stress}
  :codox {:writer codox-md.writer/write-docs
          :include [durable-queue]}
  :jvm-opts ^:replace ["-server" "-Xmx100m"])
