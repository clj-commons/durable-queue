(ns durable-queue-test
  (:require
    [clojure.java.io :as io]
    [clojure.test :refer [deftest is]]
    [durable-queue :as dq]
    [criterium.core :as c]))

(defn clear-tmp-directory []
  (doseq [f (->> (#'durable-queue/directory->queue-name->slab-files "/tmp")
              vals
              (apply concat))]
    (.delete (io/file f))))

(deftest test-basic-put-take
  (clear-tmp-directory)
  (let [q (dq/queues "/tmp" {:slab-size 1024})
        tasks (range 1e4)]
    (doseq [t tasks]
      (dq/put! q :foo t))
    (is (= tasks (map deref (dq/immediate-task-seq q :foo))))
    (dq/delete! q)))

(deftest test-partial-slab-writes
  (clear-tmp-directory)
  (dotimes [i 10]
    (dq/put! (dq/queues "/tmp") :foo i))
  (is (= (range 10) (map deref (dq/immediate-task-seq (dq/queues "/tmp") :foo)))))

(deftest test-retry
  (clear-tmp-directory)
  (with-open [^java.io.Closeable q (dq/queues "/tmp")]

    (doseq [t (range 10)]
      (dq/put! q :foo t))

    (let [tasks' (dq/immediate-task-seq q :foo)]
      (is (= (range 10) (map deref tasks')))
      (doseq [t (take 5 tasks')]
        (dq/complete! t))
      (doseq [t (range 10 15)]
        (dq/put! q :foo t))))

  ;; create a new manager, which will mark all in-progress tasks as incomplete
  (with-open [^java.io.Closeable q (dq/queues "/tmp")]
    (let [tasks' (dq/immediate-task-seq q :foo)]
      (is (= (range 5 15) (map deref tasks')))
      (doseq [t (take 5 tasks')]
        (dq/complete! t))))

  (with-open [^java.io.Closeable q (dq/queues "/tmp")]
    (let [tasks' (dq/immediate-task-seq q :foo)]
      (is (= (range 10 15) (map deref tasks')))
      (doseq [t (range 15 20)]
        (dq/put! q :foo t))))

  (let [q (dq/queues "/tmp" {:complete? even?})]
    (is (= (remove even? (range 10 20)) (map deref (dq/immediate-task-seq q :foo))))))

;;;

(deftest ^:benchmark benchmark-put-take
  (clear-tmp-directory)

  (println "\n\n-- sync both")
  (let [q (dq/queues "/tmp" {:fsync-put? true, :fsync-take? true})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync take")
  (let [q (dq/queues "/tmp" {:fsync-put? false, :fsync-take? true})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync put")
  (let [q (dq/queues "/tmp" {:fsync-put? true, :fsync-take? false})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync every 10 writes")
  (let [q (dq/queues "/tmp" {:fsync-put? false, :fsync-threshold 10})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync every 100 writes")
  (let [q (dq/queues "/tmp" {:fsync-put? false, :fsync-threshold 100})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync every 100ms")
  (let [q (dq/queues "/tmp" {:fsync-put? false, :fsync-interval 100})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo)))))

  (println "\n\n-- sync neither")
  (let [q (dq/queues "/tmp" {:fsync-put? false, :fsync-take? false})]
    (c/quick-bench
      (do
        (dq/put! q :foo 1)
        (dq/complete! (dq/take! q :foo))))))

;;;

(deftest ^:stress stress-queue-size
  (clear-tmp-directory)

  (with-open [^java.io.Closeable q (dq/queues "/tmp")]
    (let [ary (byte-array 1e6)]
      (dotimes [i 1e6]
        (aset ary i (byte (rand-int 127))))
      (dotimes [_ 1e5]
        (dq/put! q :stress ary))))

  (with-open [^java.io.Closeable q (dq/queues "/tmp" {:complete? (constantly false)})]
    (let [s (doall (dq/immediate-task-seq q :stress))]
      (doseq [t s]
        (dq/retry! t)))
    (let [s (dq/immediate-task-seq q :stress)]
      (doseq [t s]
        (dq/complete! t))))

  (clear-tmp-directory))
