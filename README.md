![](docs/EasterIsland.jpg)

This library implements a disk-backed task queue, allowing for queues that can survive processes dying, and whose size are bounded by available disk rather than memory.

### usage

```clj
[factual/durable-queue "0.1.0"]
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

Notice that the task has a value describing its progress, and a value describing the task itself.  We can get the task descriptor by dereferencing the returned task.  However, just because we've taken the task doesn't mean we've completed the action associated with it.  In order to make sure the task isn't retried on restart, we must mark it as `complete!`.

```clj
> (put! q :foo "another task")
true
> (take! q :foo)
< :in-progress | "another task" >
> (complete! *1)
true
```

If our task fails and we want to re-enqueue it to be tried again, we can instead call `(retry! task)`.

To get a description of the current state of the queue, we can use `stats`, which returns a map of queue names onto various counts:

```clj
> (stats q)
{"foo" {:enqueued 2, :retried 0, :completed 1, :in-progress 1}}
```

### configuring the queues

`queues` can be given a number of different options, which can affect its performance and correctness.  

By default, it is assumed all tasks are idempotent.  This is necessary, since the process can die at any time and leave an in-progress task in an undefined state.  If your tasks are not idempotent, a `:complete?` predicate can be defined which, on instantiation of the `queues` object, will scan through all pre-existing task descriptors and remove those for which the predicate returns true.

A complete list of options is as follows:

| name | description |
|------|-------------|
| `:complete?` | a predicate for identifying already completed tasks, defaults to always returning false |
| `:max-queue-size` | the maximum number of elements that can be in the queue before `put!` blocks |
| `:slab-size` | The size, in bytes, of the backing files for the queue.  Defaults to 16mb. |
| `:fsync-put?` | Whether an fsync should be performed for each `put!`.  Defaults to true. |
| `:fsync-take?` | Whether an fsync should be performed for each `take!`.  Defaults to false. |

Disabling `:fsync-put?` will risk losing tasks if a process dies.  Disabling `:fsync-take?` increases the chance of a task being re-run when a proces dies.  Disabling both will increase throughput of the queue by at least an order of magnitude (in the default configuration, ~1.5k tasks/sec on rotating disks and ~6k tasks/sec on SSD, with fsync completely disabled ~100k tasks/sec independent of hardware).  Tradeoffs between these two can be made by batching tasks.

### license

Copyright © 2013 Factual Inc

Distributed under the Eclipse Public License 1.0
