![](docs/EasterIsland.jpg)

This library implements a disk-backed task queue, allowing for queues that can survive processes dying, and whose size is bounded by available disk rather than memory.  It is a small, purely-Clojure implementation focused entirely on the in-process use case, meaning that it is both simpler and more easily embedded than network-aware queue implementations such as Kafka and ActiveMQ.

### usage

```clj
[factual/durable-queue "0.1.5"]
```

To interact with queues, first create a `queues` object by specifying a directory in the filesystem and an options map:

```clj
> (require '[durable-queue :refer :all])
nil
> (def q (queues "/tmp" {}))
#'q
```

This allows us to `put!` and `take!` tasks from named queues.  `take!` is a blocking read, and will only return once a task is available or, if a timeout is defined (in milliseconds), once the timeout elapses:

```clj
> (take! q :foo 10 :timed-out!)
:timed-out!
> (put! q :foo "a task")
true
> (take! q :foo)
< :in-progress | "a task" >
> (deref *1)
"a task"
```

Notice that the task has a value describing its progress, and a value describing the task itself.  We can get the task descriptor by dereferencing the returned task.  Note that since the task is persisted to disk and anything on disk may be corrupted, this involves a checksum which may fail and throw an `IOException`.  Any software which wants to be robust to all failure modes should always dereference within a `try`/`catch` clause.

Calling `take!` removed the task from the queue, but just because we've taken the task doesn't mean we've completed the action associated with it.  In order to make sure the task isn't retried on restart, we must mark it as `complete!`.

```clj
> (put! q :foo "another task")
true
> (take! q :foo)
< :in-progress | "another task" >
> (complete! *1)
true
```

If our task fails and we want to re-enqueue it to be tried again, we can instead call `(retry! task)`.  Tasks which are marked for retry are added to the end of the current queue.

To get a description of the current state of the queue, we can use `stats`, which returns a map of queue names onto various counts:

```clj
> (stats q)
{:enqueued 2,
 :retried 0,
 :completed 1,
 :in-progress 1,
 :num-slabs 1,
 :num-active-slabs 1}
```

| field | description |
|-------|-------------|
| `:enqueued` | the number of tasks which have been enqueued via `put!`, including any pre-existing tasks on-disk when the queues were initialized |
| `:retried` | the number of tasks which have been retried via `retry!` |
| `:completed` | the number of tasks which have been completed via `complete!` |
| `:in-progress` | the number of tasks which have been consumed via `take!`, but are not yet complete |
| `:num-slabs` | the number of underlying files which are being used to store tasks |
| `:num-active-slabs` | the number of underlying files which are currently open and mapped into memory |

### configuring the queues

`queues` can be given a number of different options, which can affect its performance and correctness.

By default, it is assumed all tasks are idempotent.  This is necessary, since the process can die at any time and leave an in-progress task in an undefined state.  If your tasks are not idempotent, a `:complete?` predicate can be defined which, on instantiation of the `queues` object, will scan through all pre-existing task descriptors and remove those for which the predicate returns true.

A complete list of options is as follows:

| name | description |
|------|-------------|
| `:complete?` | a predicate for identifying already completed tasks, defaults to always returning false |
| `:max-queue-size` | the maximum number of elements that can be in the queue before `put!` blocks, defaults to `Integer/MAX_VALUE`      |
| `:slab-size` | The size, in bytes, of the backing files for the queue.  The size of a serialized task cannot be larger than this size, defaults to 64mb. |
| `:fsync-put?` | Whether an fsync should be performed for each `put!`.  Defaults to `true`. |
| `:fsync-take?` | Whether an fsync should be performed for each `take!`.  Defaults to `false`. |
| `:fsync-threshold` | The maximum number of writes (puts, takes, retries, completes) that can be performed before an fsync is performed. |
| `:fsync-interval` | The maximum amount of time, in milliseconds, that can elapse before an fsync is performed. |

Disabling `:fsync-put?` will risk losing tasks if a process dies.  Disabling `:fsync-take?` increases the chance of a task being re-run when a process dies.  Disabling both will increase throughput of the queue by at least an order of magnitude (in the default configuration, ~1.5k tasks/sec on rotating disks and ~6k tasks/sec on SSD, with fsync completely disabled ~100k tasks/sec independent of hardware).

Writes can be batched using `fsync-threshold` and/or `fsync-interval`, or by explicitly calling `(durable-queue/fsync q)`.  Setting the `fsync-threshold` to 10 will allow for ~25k tasks/sec on SSD, and still enforces a small upper boundary on how much data can be lost when the process dies.  An exception will be thrown if both per-task and batch sync options are set.

### license

Copyright © 2015 Factual Inc

Distributed under the Eclipse Public License 1.0
