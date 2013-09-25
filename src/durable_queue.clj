(ns durable-queue
  (:require
    [clojure.java.io :as io]
    [byte-streams :as bs]
    [clojure.string :as str]
    [taoensso.nippy :as nippy])
  (:import
    [java.util.concurrent
     LinkedBlockingQueue
     TimeoutException
     TimeUnit]
    [java.io
     Writer
     File
     RandomAccessFile
     IOException]
    [java.nio.channels
     FileChannel
     FileChannel$MapMode]
    [java.nio
     ByteBuffer
     MappedByteBuffer]))

;;;

(defprotocol ITask
  (^:private status [_] "Returns the task status")
  (^:private status! [_ status] "Sets the task status"))

;; a single task within a slab, assumes that the buffer is sliced around
;; the task's boundaries
(defrecord Task [^MappedByteBuffer buf task-ref status-callback]
  clojure.lang.IDeref
  (deref [_] @task-ref)
  ITask
  (status [_]
    (case (.get buf 1)
      0 :incomplete
      1 :in-progress
      2 :complete))
  (status! [_ status]
    (.put buf 1
      (case status
        :incomplete 0
        :in-progress 1
        :complete 2))
    (when status-callback
      (status-callback status))
    nil))

(defmethod print-method Task [t ^Writer w]
  (.write w
    (str "< " (status t) " | " (pr-str @t) " >")))

;; the byte layout is
;; [ exists? : int8
;;   state   : int8 
;;   size    : int32 
;;   payload : array ]
;; valid values for 'exists' is 0 (no), 1 (yes)
;; valid values for 'state' is 0 (unclaimed), 1 (in progress), 2 (complete)
(defn- buf->task-seq [^ByteBuffer buf]
  (lazy-seq
    (let [buf' (.duplicate buf)]
      (when (and
              (pos? (.remaining buf'))
              (== 1 (.get buf')))
        (let [status (.get buf')
              size (.getInt buf')
              task-buf (-> buf
                         .duplicate
                         ^ByteBuffer
                         (.limit (+ (.position buf) 6 size))
                         .slice)]
          (cons
            (Task.
              task-buf
              (delay
                (nippy/thaw-from-bytes
                  (-> task-buf
                    .duplicate
                    (.position 6)
                    bs/to-byte-array)))
              nil)
            (buf->task-seq
              (-> buf
                .duplicate
                (.position (+ (.position buf) 6 size))))))))))

;;;

(defprotocol ITaskSlab
  (^:private append-to-slab! [_ descriptor]))

(deftype TaskSlab [filename ^MappedByteBuffer buf]
  ITaskSlab
  (append-to-slab! [this descriptor]
    (locking this
      (let [ary (nippy/freeze descriptor)
            cnt (count ary)]
        (when (> (.remaining buf) (+ (count ary) 6))
          (let [pos (.position buf)]

            ;; write to the buffer
            (doto buf
              (.put (byte 1)) ;; exists
              (.put (byte 0)) ;; incomplete
              (.putInt cnt)
              (.put ary)
              (.put (byte 0))            ;; next doesn't exist
              (.position (+ pos cnt 6))) ;; position write marker for next task

            ;; return a task to enqueue in-memory
            (let [task-buf (-> buf
                             .duplicate
                             (.position pos)
                             ^ByteBuffer (.limit (+ pos cnt 6))
                             .slice)]
              (with-meta
                (Task.
                  task-buf
                  (delay
                    (nippy/thaw-from-bytes
                      (-> task-buf
                        .duplicate
                        (.position 6)
                        bs/to-byte-array)))
                  nil)
                {::slab this})))))))
  clojure.lang.Seqable
  (seq [this]
    (map #(vary-meta % assoc ::slab this)
      (-> buf
        .duplicate
        (.position 0)
        buf->task-seq)))
  Comparable
  (compareTo [_ x]
    (assert (instance? TaskSlab x))
    (compare filename (.filename ^TaskSlab x))))

(def ^:private fs-monitor (Object.))

(defn- delete-slab
  [^TaskSlab slab]
  (locking fs-monitor
    (.delete (io/file (.filename slab)))))

(defn- sync-slab
  [^TaskSlab slab]
  (.force ^MappedByteBuffer (.buf slab)))

(defn- create-slab
  "Creates a new slab file, ensuring a new file name that is lexicographically greater than
   any existing files for that queue name."
  ([directory q-name size]
     (locking fs-monitor
       (let [pattern (re-pattern (str "^" q-name "_(\\d+)"))
             last-number (->> directory
                           io/file
                           .listFiles
                           (map #(.getName ^File %))
                           (map #(second (re-find pattern %)))
                           (remove nil?)
                           (map #(Long/parseLong %))
                           sort
                           last)
             n (if last-number (inc last-number) 0)
             f (io/file (str directory "/" q-name "_" n))]

         (when-not (.createNewFile f)
           (throw (IOException. (str "Could not create new slab file at " (.getAbsolutePath f)))))

         (let [raf (doto (RandomAccessFile. f "rw")
                     (.setLength size))
               buf (-> raf
                     .getChannel
                     (.map FileChannel$MapMode/READ_WRITE 0 size))]
           (doto buf
             (.put 0 (byte 0))
             .force)
           (TaskSlab. (.getAbsolutePath f) buf))))))

(defn- file->slab
  "Transforms a file into a slab representing that file's contents."
  [filename]
  (let [raf (RandomAccessFile. (io/file filename) "rw")
        buf (-> raf 
              .getChannel
              (.map FileChannel$MapMode/READ_WRITE 0 (.length raf))) 
        len (->> buf
              buf->task-seq
              (map :buf)
              (map #(.remaining ^ByteBuffer %))
              (reduce +))]
    (TaskSlab. filename (.position buf len))))

(defn- directory->queue->slab-files
  "Returns a map of queue names onto slab files for that queue."
  [directory]
  (let [queue->file (->> directory
                      io/file
                      .listFiles
                      (filter #(re-find #"\w+_\d+" (.getName ^File %)))
                      (group-by #(second (re-find #"(\w+)_\d+" (.getName ^File %)))))]
    (zipmap
      (keys queue->file)
      (map
        (fn [files]
          (->> files
            (map #(.getAbsolutePath ^File %))
            sort))
        (vals queue->file)))))

;;;

(defprotocol IQueueManager
  (take!
    [_ q-name]
    [_ q-name timeout timeout-val]
    "A blocking dequeue from `name`.  If `timeout` is specified, returns `timeout-val` if
     no task is available within `timeout` milliseconds.")
  (put!
    [_ q-name task-descriptor]
    [_ q-name task-descriptor timeout]
    "A blocking enqueue to `name`.  If `timeout` is specified, returns `false` if unable to
     enqueue within `timeout` milliseconds."))

(defn queue-manager
  "Creates a point of interaction for queues, backed by disk storage in `directory`.

   The following options can be specified:

       max-queue-size - the maximum number of elements that can be in the queue before `put!`
                        blocks.  Defaults to `Integer/MAX_VALUE`.

       complete? - a predicate that is run on pre-existing tasks to check if they were already
                   completed.  If the tasks in the queue are non-idempotent, this must be
                   specified for correct behavior.  Defaults to always returning false.

       slab-size - The size, in bytes, of the backing files for the queue.  Defaults to 16mb.

       fsync-put? - if true, each `put!` will force an fsync.  Defaults to true.

       fsync-take? - if true, each `take!` will force an fsync.  Defaults to false."
  ([directory]
     (queue-manager directory nil))
  ([directory
    {:keys [max-queue-size
            complete?
            slab-size
            fsync-put?
            fsync-take?]
     :or {max-queue-size Integer/MAX_VALUE
          complete? (constantly false)
          slab-size (* 16 1024 1024)
          fsync-put? true
          fsync-take? false}}]

     (let [queue (memoize (fn [_] (LinkedBlockingQueue. (int max-queue-size))))
           queue->files (directory->queue->slab-files directory)
           queue->slabs (atom
                          (zipmap
                            (keys queue->files)
                            (->> queue->files
                              vals
                              (map #(map file->slab %))
                              vec)))
           slabs (->> @queue->slabs vals (apply concat))
           slab->count (zipmap
                         slabs
                         (map #(atom (count (seq %))) slabs))
           create-new-slab (fn [q-name]
                             (let [slab (create-slab directory q-name slab-size)
                                   empty-slabs (filter empty? (@queue->slabs q-name))]
                               (doseq [s empty-slabs]
                                 (delete-slab s)) 
                               (swap! queue->slabs update-in [q-name]
                                 #(conj (vec (remove empty? %)) slab))
                               slab))]

       ;; populate queues with pre-existing tasks
       (doseq [[q slabs] @queue->slabs]
         (let [^LinkedBlockingQueue q' (queue q)]
           (doseq [slab slabs]
             (let [tasks (->> slab
                           seq
                           (map #(vary-meta % assoc ::queue q' ::fsync? fsync-take?))
                           (remove #(or (= :complete (status %)) (complete? @%))))]

               (if (empty? tasks)

                 ;; if there aren't any active tasks, just delete the slab
                 (delete-slab slab)

                 (doseq [task tasks]
                   (status! task :incomplete)
                   (when-not (.offer q' task)
                     (throw
                       (IllegalArgumentException.
                         "'max-queue-size' insufficient to hold existing tasks."))))))
             (sync-slab slab))))

       (reify IQueueManager

         (take! [this q-name timeout timeout-val]
           (let [q-name (munge (name q-name))
                 ^LinkedBlockingQueue q (queue q-name)]
             (try
               (if-let [t (if (zero? timeout)
                            (.poll q)
                            (.poll q timeout TimeUnit/MILLISECONDS))]
                 (do
                   (status! t :in-progress)
                   (when fsync-take?
                     (sync-slab (-> t meta ::slab)))
                   t)
                 timeout-val)
               (catch TimeoutException _
                 timeout-val))))
         (take! [this q-name]
           (take! this q-name Long/MAX_VALUE nil))

         (put! [_ q-name task-descriptor timeout]
           (let [q-name (munge (name q-name))
                 ^LinkedBlockingQueue q (queue q-name)
                 slab! (fn []
                         (let [slabs (@queue->slabs q-name)
                               slab  (last slabs)
                               task  (when slab
                                       (append-to-slab! slab task-descriptor))

                               ;; if no task was created, we need to create a new slab file
                               ;; and try again
                               slab  (if task
                                       slab
                                       (create-new-slab q-name))
                               task  (or task (append-to-slab! slab task-descriptor))]
                           (when fsync-put?
                             (sync-slab slab))
                           task))
                 
                 queue! (fn [task]
                          (if (zero? timeout)
                            (.offer q task)
                            (.offer q task timeout TimeUnit/MILLISECONDS)))]
             (locking q
               (queue!
                 (vary-meta (slab!) assoc
                   ::queue q
                   ::fsync? fsync-take?)))))
         (put! [this q-name task-descriptor]
           (put! this q-name task-descriptor Long/MAX_VALUE))))))

;;;

(defn task-seq
  "Returns an infinite lazy sequence of tasks for `q-name`."
  [q-manager q-name]
  (lazy-seq
    (cons
      (take! q-manager q-name)
      (task-seq q-manager q-name))))

(defn immediate-task-seq
  "Returns a finite lazy sequence of tasks for `q-name` which terminates once there are
   no more tasks immediately available."
  [q-manager q-name]
  (lazy-seq
    (let [task (take! q-manager q-name 0 ::none)]
      (when-not (= ::none task)
        (cons
          task
          (immediate-task-seq q-manager q-name))))))

(defn complete!
  "Marks a task as complete."
  [task]
  (status! task :complete)
  (when (-> task meta ::fsync?)
    (sync-slab (-> task meta ::slab)))
  true)

(defn retry!
  "Marks a task as available for retry."
  [task]
  (status! task :incomplete)
  (when (-> task meta ::fsync?)
    (sync-slab (-> task meta ::slab)))
  (let [^LinkedBlockingQueue q (-> task meta ::queue)]
    (.offer q
      task
      Long/MAX_VALUE
      TimeUnit/MILLISECONDS)))
