## CHANGES

v0.2.0 in progress
* Deprecated single-segment `durable-queue` ns; use `clj-commons.durable-queue` instead
* Clean up lint issues; export clj-kondo config
* Add a changelog!

v0.1.7 -- 2025-09-02
* Bring all dependencies up to date
* Matrix test against Clojure 1.10, 1.11, and 1.12
* Matrix test against JDK 11, 17, and 21 (also tested on JDK 24 manually)
* Add `deps.edn`, `bb.edn`, `build.clj`
* Add GitHub Actions for test, snapshot, and release
* Remove non-functional CircleCI integration
