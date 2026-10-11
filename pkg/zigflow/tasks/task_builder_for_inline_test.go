/*
 * Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"fmt"
	"testing"

	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"github.com/zigflow/zigflow/pkg/zigflow/flow"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	// testConstInlineForTask is the for task key used by the inline for tests.
	testConstInlineForTask = "loop"
	// testConstInnerForTask is the key of the for task nested in the loop body.
	testConstInnerForTask = "inner"
	// testConstCaughtFlag is the data key set by the catch body of a try task
	// nested in a loop body.
	testConstCaughtFlag = "caught"
)

// newInlineForBuilder returns a for builder over $data.items whose iterations
// run the supplied body.
func newInlineForBuilder(body TemporalWorkflowFunc) *ForTaskBuilder {
	return &ForTaskBuilder{
		doc:          testWorkflow,
		eventEmitter: testEvents,
		name:         testConstInlineForTask,
		task: &model.ForTask{
			For: model.ForTaskConfiguration{Each: constDefaultItemVar, At: testConstIdx, In: testConstForRefDataItems},
			Do:  &model.TaskList{&model.TaskItem{Key: testConstStep, Task: &model.DoTask{}}},
		},
		inlineBody: body,
	}
}

func runForExec(t *testing.T, b *ForTaskBuilder, state *utils.State) (output any, err error) {
	t.Helper()

	fn, buildErr := b.exec()
	require.NoError(t, buildErr)

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		output, err = fn(ctx, nil, state)
		return output, err
	}, workflow.RegisterOptions{Name: "for-inline"})

	env.ExecuteWorkflow("for-inline")

	return output, err
}

// TestForExecIteratesObjectKeysInSortedOrder proves iteration over an object is
// deterministic, as Go randomises map order. There are enough keys that an
// unsorted iteration would not match by chance.
func TestForExecIteratesObjectKeysInSortedOrder(t *testing.T) {
	const keyCount = 20

	items := make(map[string]any, keyCount)
	want := make([]any, 0, keyCount)
	for i := range keyCount {
		key := fmt.Sprintf("key-%02d", i)
		items[key] = i
		want = append(want, key)
	}

	var seen []any
	b := newInlineForBuilder(func(ctx workflow.Context, input any, st *utils.State) (any, error) {
		seen = append(seen, st.Data[testConstIdx])
		return nil, nil
	})

	state := utils.NewState()
	state.AddData(map[string]any{testConstItems: items})

	_, err := runForExec(t, b, state)
	require.NoError(t, err)

	assert.Equal(t, want, seen)
}

// TestForExecEndPropagatesOutput proves a `then: end` inside an iteration stops
// the loop and carries the iteration's output out as the for task's output.
func TestForExecEndPropagatesOutput(t *testing.T) {
	calls := 0
	b := newInlineForBuilder(func(ctx workflow.Context, input any, st *utils.State) (any, error) {
		calls++
		return map[string]any{testConstValue: testConstEnded}, flow.ErrEnd
	})

	state := utils.NewState()
	state.AddData(map[string]any{testConstItems: []any{"x", "y"}})

	output, err := runForExec(t, b, state)

	require.ErrorIs(t, err, flow.ErrEnd)
	assert.Equal(t, map[string]any{testConstValue: testConstEnded}, output)
	assert.Equal(t, map[string]any{testConstValue: testConstEnded}, state.Output)
	assert.Equal(t, 1, calls, "the loop must stop at the iteration that ended the workflow")
}

func newLoopTask(in, each string, body *model.TaskList) *model.ForTask {
	return &model.ForTask{
		For: model.ForTaskConfiguration{Each: each, In: in},
		Do:  body,
	}
}

// TestBuiltForWithCaughtFailureRunsInline drives a real built workflow in which
// every iteration of a for task fails inside a try body. Each failure is caught
// in the enclosing workflow and the loop carries on. The bodies are not
// registered with the test environment as workflows, so starting any of them
// as a child workflow would fail the run.
func TestBuiltForWithCaughtFailureRunsInline(t *testing.T) {
	doc := newTestWorkflowDoc("wf-inline-for-try")
	events, readStarted := newStartedTaskRecorder(t, doc)

	tryTask := &model.TryTask{
		Try: &model.TaskList{&model.TaskItem{Key: testConstFailingTask, Task: newFailingRaiseTask()}},
		Catch: &model.TryTaskCatch{
			Do: &model.TaskList{&model.TaskItem{Key: testConstCaughtFlag, Task: newSetTask(map[string]any{testConstCaughtFlag: true})}},
		},
	}
	do := &model.TaskList{
		&model.TaskItem{Key: testConstInlineForTask, Task: newLoopTask(
			"${ [1, 2, 3] }", constDefaultItemVar,
			&model.TaskList{&model.TaskItem{Key: testConstTryTask, Task: tryTask}},
		)},
	}

	fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)
	output, err := runRootWorkflow(t, doc.Document.Name, fn, nil)

	require.NoError(t, err)
	caught := map[string]any{testConstCaughtFlag: true}
	assert.Equal(t, []any{caught, caught, caught}, output)
	assert.Len(t, readStarted(), 1+3*3, "the loop task plus a try, failing and catch task for each of the three iterations")
}

// TestBuiltNestedForRunsInline proves a for task nested in another for task
// runs inline, and that the inner loop sees both its own and the outer loop's
// variables.
func TestBuiltNestedForRunsInline(t *testing.T) {
	doc := newTestWorkflowDoc("wf-inline-for-for")
	events, _ := newStartedTaskRecorder(t, doc)

	inner := newLoopTask("${ [\"a\", \"b\"] }", "inner", &model.TaskList{
		&model.TaskItem{Key: testConstStep, Task: newSetTask(map[string]any{
			testConstValue: "${ $data.inner }",
			testConstStage: "${ $data.outer }",
		})},
	})
	do := &model.TaskList{
		&model.TaskItem{Key: testConstInlineForTask, Task: newLoopTask(
			"${ [1, 2] }", "outer",
			&model.TaskList{&model.TaskItem{Key: testConstInnerForTask, Task: inner}},
		)},
	}

	fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)
	output, err := runRootWorkflow(t, doc.Document.Name, fn, nil)

	require.NoError(t, err)
	assert.Equal(t, []any{
		[]any{
			map[string]any{testConstValue: "a", testConstStage: float64(1)},
			map[string]any{testConstValue: "b", testConstStage: float64(1)},
		},
		[]any{
			map[string]any{testConstValue: "a", testConstStage: float64(2)},
			map[string]any{testConstValue: "b", testConstStage: float64(2)},
		},
	}, output)
}

// TestBuiltForBodyEndTerminatesWholeWorkflow proves `then: end` in a loop body
// ends the whole workflow: the loop stops at the iteration that ended it and the
// task after the loop does not run. It builds the real workflow, so it also
// covers the loop body being marked as running inline.
func TestBuiltForBodyEndTerminatesWholeWorkflow(t *testing.T) {
	doc := newTestWorkflowDoc("wf-inline-for-end")
	events, readStarted := newStartedTaskRecorder(t, doc)

	do := &model.TaskList{
		&model.TaskItem{Key: testConstInlineForTask, Task: newLoopTask(
			"${ [1, 2, 3] }", constDefaultItemVar,
			&model.TaskList{&model.TaskItem{Key: testConstFinishTask, Task: newEndingSetTask(map[string]any{
				testConstResult: testConstEnded,
			})}},
		)},
		&model.TaskItem{Key: testConstMustNotRun, Task: newSetTask(map[string]any{testConstTaskAfter: true})},
	}

	fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)
	output, err := runRootWorkflow(t, doc.Document.Name, fn, nil)

	require.NoError(t, err)
	assert.Equal(t, map[string]any{testConstResult: testConstEnded}, output)
	assert.Equal(t, []string{testConstInlineForTask, testConstFinishTask}, readStarted())
}

// TestBuiltNestedDoEndTerminatesWholeWorkflow proves that `then: end` inside a
// do task nested in an inline try or loop body ends the whole workflow. The
// nested task list shares the workflow's Temporal context, so it must be built
// as nested too, or it consumes the end and the tasks after it keep running.
func TestBuiltNestedDoEndTerminatesWholeWorkflow(t *testing.T) {
	innerDo := func() *model.DoTask {
		return &model.DoTask{Do: &model.TaskList{
			&model.TaskItem{Key: testConstFinishTask, Task: newEndingSetTask(map[string]any{
				testConstResult: testConstEnded,
			})},
		}}
	}
	body := func() *model.TaskList {
		return &model.TaskList{
			&model.TaskItem{Key: testConstInnerDo, Task: innerDo()},
			&model.TaskItem{Key: testConstStep, Task: newSetTask(map[string]any{testConstValue: true})},
		}
	}

	tests := map[string]model.Task{
		"for": newLoopTask("${ [1, 2] }", constDefaultItemVar, body()),
		"try": &model.TryTask{
			Try:   body(),
			Catch: &model.TryTaskCatch{Do: &model.TaskList{&model.TaskItem{Key: testConstCaughtFlag, Task: newSetTask(nil)}}},
		},
	}

	for name, task := range tests {
		t.Run(name, func(t *testing.T) {
			doc := newTestWorkflowDoc("wf-nested-do-end-" + name)
			events, readStarted := newStartedTaskRecorder(t, doc)

			do := &model.TaskList{
				&model.TaskItem{Key: testConstInlineForTask, Task: task},
				&model.TaskItem{Key: testConstMustNotRun, Task: newSetTask(map[string]any{testConstTaskAfter: true})},
			}

			fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)
			output, err := runRootWorkflow(t, doc.Document.Name, fn, nil)

			require.NoError(t, err)
			assert.Equal(t, map[string]any{testConstResult: testConstEnded}, output)
			assert.NotContains(t, readStarted(), testConstMustNotRun)
			assert.NotContains(t, readStarted(), testConstStep)
		})
	}
}

// TestBuiltInlineBodiesEmitOneWorkflowLifecycle proves the inline loop and try
// bodies do not report a second workflow start or completion for the workflow
// they run in.
func TestBuiltInlineBodiesEmitOneWorkflowLifecycle(t *testing.T) {
	doc := newTestWorkflowDoc("wf-inline-lifecycle")
	events, readEvents := newRecordingEventsWithPayload(t)

	tryTask := &model.TryTask{
		Try:   &model.TaskList{&model.TaskItem{Key: testConstStep, Task: newSetTask(map[string]any{testConstValue: true})}},
		Catch: &model.TryTaskCatch{Do: &model.TaskList{&model.TaskItem{Key: testConstCaughtFlag, Task: newSetTask(nil)}}},
	}
	do := &model.TaskList{
		&model.TaskItem{Key: testConstInlineForTask, Task: newLoopTask(
			"${ [1, 2] }", constDefaultItemVar,
			&model.TaskList{&model.TaskItem{Key: testConstTryTask, Task: tryTask}},
		)},
	}

	fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)
	_, err := runRootWorkflow(t, doc.Document.Name, fn, nil)
	require.NoError(t, err)

	types := recordedEventTypes(readEvents())
	assert.Equal(t, 1, countOf(types, testConstWorkflowStartedEvent))
	assert.Equal(t, 1, countOf(types, testConstWorkflowCompletedEvent))
}
