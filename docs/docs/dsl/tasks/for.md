# For

Iterates over a collection and executes a set of tasks for each item.

## When to use this

Use For when you need to run the same set of tasks for every item
in a collection. The collection can be an array, a map or an
integer count.

## Properties

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| for.each | `string` | `no` | The name of the variable used to store the current item being enumerated.<br />Defaults to `item`. |
| for.in | `string` | `yes` | A [runtime expression](/docs/dsl/tasks/intro#runtime-expressions) used to get the collection to enumerate. |
| for.at | `string` | `no` | The name of the variable used to store the index of the current item being enumerated.<br />Defaults to `index`. |
| while | `string` | `no` | A [runtime expression](/docs/dsl/tasks/intro#runtime-expressions) that represents the condition, if any, that must be met for the iteration to continue.<br />The result of each iteration is stored in `$data.<taskName>`, allowing the while expression to conditionally stop the loop. |
| do | [`map[string, task]`](/docs/dsl/tasks/intro) | `yes` | The [task(s)](/docs/dsl/tasks/intro) to perform for each item in the collection. These will be run in the same workflow, one iteration after another. |

## Example

```yaml
document:
  dsl: 1.0.0
  taskQueue: zigflow # Mapped to the task queue
  workflowType: for-loop # Workflow name
  version: 0.0.1
do:
  # Iterate over the map object
  - forTaskMap:
      export:
        as: '${ $context + { forTaskMap: . } }'
      for:
        in: ${ $input.map }
      do:
        - setData:
            export:
              as: ${ . }
            set:
              key: "${ \"hello: \" + $data.index }"
              value: ${ $data.item }
        - wait:
            output:
              as: ${ $context }
            wait:
              seconds: 2
  # Iterate over the data array
  - forTaskArray:
      export:
        as: '${ $context + { forTaskArray: . } }'
      for:
        each: item
        in: ${ $input.data }
        at: index
      # while: ${ $data.item.userId != 4 } # If this returns false, it will cut the iteration
      do:
        # Each iteration will run these tasks in order
        - setData:
            export:
              as: ${ . }
            set:
              userId: ${ $data.item.userId } # Get the userId for this iteration
              id: ${ $data.index } # Get the key
              processed: true
        - wait:
            output:
              as: ${ $context }
            wait:
              seconds: 1
  - forTaskNumber:
      output:
        as: '${ $context + { forTaskNumber: . } }'
      for:
        in: ${ 5 }
      do:
        - setData:
            export:
              as: ${ . }
            set:
              number: ${ $data.item }
        - wait:
            output:
              as: ${ $context }
            wait:
              seconds: 1
```

## Gotchas

**Each iteration runs in the same workflow.** The tasks of every iteration are
added to the history of the workflow that contains the loop, so a loop over a
large collection grows that history. Zigflow continues as new between the
top-level tasks of a workflow, not part way through a loop, so a single loop
must fit within Temporal's
[event history limits](https://docs.temporal.io/workflow-execution/event).
Temporal terminates a workflow that exceeds them. Keep loops bounded, or use a
[`run`](/docs/dsl/tasks/run) task with `workflow` to start a child workflow when
each item needs its own history. For a very large collection, split it into
batches and start one child workflow per batch with `run` and `workflow`, so
each batch gets its own history.

**`listen` handlers in the loop body belong to the whole workflow.** The body
runs inline in it, so a query, signal or update `id` shares one namespace with
every other `listen` task in the workflow. A query or update handler replaces
any earlier handler with the same `id`, including the one registered by the
previous iteration, and a signal is consumed by whichever `listen` task with
that `id` receives it first. Use `id` values that no other `listen` task in the
workflow uses.

**Object keys are iterated in sorted order.** The keys are compared as strings.
This keeps the iteration order the same every time the workflow is replayed.

**The `while` condition is evaluated before each iteration.** It is checked
before the first iteration too, so a condition that is false at the start runs
no iterations.

**Iteration results are stored under `$data.<taskName>`.** The `while`
expression can access this key to implement early termination.

:::warning Migration note
Earlier versions ran each iteration as a child workflow. A workflow that is
still running and has already started an iteration as a child workflow fails
when it replays on the new version. Let such workflows finish or continue as new
before you upgrade, or use
[Temporal Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
to keep each running workflow on the worker build it started on. See
[Temporal Worker Controller](/docs/deployment/kubernetes#temporal-worker-controller)
for versioned rollouts.
:::

## Related pages

- [Fork](/docs/dsl/tasks/fork): parallel execution of multiple branches
- [Do](/docs/dsl/tasks/do): sequential subtasks
- [Set](/docs/dsl/tasks/set): storing iteration data
- [Concepts: data and expressions](/docs/concepts/data-and-expressions):
  runtime expressions and `$data` variables
- [Examples: parallel tasks](/docs/examples/parallel-tasks):
  running branches concurrently
