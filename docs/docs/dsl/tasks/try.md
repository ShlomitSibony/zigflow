# Try

Serves as a mechanism within workflows to handle errors gracefully, potentially
retrying failed tasks before proceeding with alternate ones.

## When to use this

Use Try when a task may fail and you want to recover without
failing the entire workflow. Common uses include optional HTTP
calls and handling external service errors.

## Properties {/*#try-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| try | [`map[string, task]`](/docs/dsl/tasks/intro) | `yes` | The task(s) to perform. These will be run in the same workflow. |
| catch | [`catch`](#catch) | `yes` | Configures the errors to catch and how to handle them. |

## Example

```yaml
document:
  dsl: 1.0.0
  taskQueue: zigflow
  workflowType: example
  version: 0.0.1
do:
  - user:
      try:
        - getUser:
            call: http
            output:
              as:
                user: ${ . }
            with:
              method: get
              # This URL returns a 404
              endpoint: https://jsonplaceholder.typicode.com/users/2000
      catch:
        do:
          - setError:
              output:
                as:
                  error: ${ . }
              set:
                message: some error
```

This outputs:

```json
{
  "user": {
    "error": {
      "message": "some error"
    }
  }
}
```

## Definitions

### Catch

Defines the configuration of a catch clause, which a concept used to catch
errors.

#### Properties {/*#catch-properties*/}

| Name | Type | Required | Description |
| --- | :---: | :---: | --- |
| do | [`map[string, task]`](/docs/dsl/tasks/intro) | `yes` | The definition of the task(s) to run when catching an error. These will be run in the same workflow. |
| as | `string` | `no` | The key under `$data` where the caught error is stored. Defaults to `error`. |

## Gotchas

**The `catch` block catches all errors.** There is no filtering by error type.
To handle different error types differently, inspect the error object inside
the `catch` block. See
[the error object](/docs/concepts/error-handling-and-retries#the-error-object)
for the fields it carries.

**The `try` block runs in the same workflow.** Its tasks are added to the
history of the workflow that contains it. The retry policy configured on inner
tasks still applies before the `catch` block runs.

**`listen` handlers in the `try` or `catch` block belong to the whole
workflow.** The blocks run inline in it, so a query, signal or update `id`
shares one namespace with every other `listen` task in the workflow. A query or
update handler replaces any earlier handler with the same `id`, and a signal is
consumed by whichever `listen` task with that `id` receives it first. Use `id`
values that no other `listen` task in the workflow uses.

**State changes made inside the `try` block do not leave it.** Values that its
tasks set in `$data` or `$context` are discarded, whether the block succeeds or
fails. The `catch` block and the tasks after the `try` task start from the state
the `try` task started with. Only the output of the `try` block, or of the
`catch` block when it runs, becomes the output of the `try` task, so use
`output` or `export` on the `try` task to keep a value.

**Cancelling the workflow is not caught.** The `catch` block does not run when
the workflow is cancelled.

:::warning Migration note
Earlier versions ran the `try` and `catch` blocks as child workflows. A workflow
that is still running and has already started the `try` or `catch` block as a
child workflow fails when it replays on the new version. Let such workflows
finish or continue as new before you upgrade, or use
[Temporal Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)
to keep each running workflow on the worker build it started on. See
[Temporal Worker Controller](/docs/deployment/kubernetes#temporal-worker-controller)
for versioned rollouts.
:::

## Related pages

- [Raise](/docs/dsl/tasks/raise): raising explicit errors
- [Call](/docs/dsl/tasks/call): HTTP and activity calls that may fail
- [Concepts: error handling and retries](/docs/concepts/error-handling-and-retries):
  error model overview
- [Examples: error handling](/docs/examples/error-handling):
  try/catch walkthrough
