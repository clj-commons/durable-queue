## CHANGES

v0.2.0 -- 2025-11-02
* **Removed single-segment `durable-queue` ns; use `clj-commons.durable-queue` instead**
* Deprecate `delete!` because it is dangerous: it leaves queues in a corrupted state (because it deletes files without cleaning up in-memory state).
* Address [#30](https://github.com/clj-commons/durable-queue/issues/30) by adding `delete-queue!` and `delete-all!` to safely delete an individual queue (and its files) and safely delete all queues (and their files).
* Clean up lint issues; export clj-kondo config
* Update dependencies (again)
* Add a changelog!

v0.1.7 -- 2025-09-02
* Bring all dependencies up to date
* Matrix test against Clojure 1.10, 1.11, and 1.12
* Matrix test against JDK 11, 17, and 21 (also tested on JDK 24 manually)
* Add `deps.edn`, `bb.edn`, `build.clj`
* Add GitHub Actions for test, snapshot, and release
* Remove non-functional CircleCI integration
