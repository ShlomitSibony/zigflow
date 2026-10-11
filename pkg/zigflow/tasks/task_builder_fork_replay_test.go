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
	"testing"

	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// replayForkWorkflowName is the workflow type recorded in the history under
// testdata.
const replayForkWorkflowName = "workflow"

// replayForkAttempts is how many times the history is replayed. Without the
// ordered cancellation each replay had a chance of failing, so a single
// replay could pass by luck.
const replayForkAttempts = 30

// TestForkCompeteReplaysItsRecordedCancelOrder replays the history of a
// competing fork with four branches, in which the first branch (gamma) wins and the
// other three are cancelled.
//
// Replay checks the cancel commands against the history in order, so the fork
// has to cancel its losing branches in the same order on every run. It used to
// walk a map, so the order changed from run to run and replay failed with a
// non-determinism error most of the time.
//
// The history was recorded from a local run of this workflow:
//
//	do:
//	  - race:
//	      fork:
//	        compete: true
//	        branches:
//	          - gamma: {set: {winner: gamma}}
//	          - delta: {wait: {seconds: 20}}
//	          - epsilon: {wait: {seconds: 20}}
//	          - zeta: {wait: {seconds: 20}}
//	  - after: {set: {done: true}}
func TestForkCompeteReplaysItsRecordedCancelOrder(t *testing.T) {
	doc := newTestWorkflowDoc(replayForkWorkflowName)
	events, _ := newStartedTaskRecorder(t, doc)

	wait := func() *model.WaitTask {
		return &model.WaitTask{Wait: &model.Duration{Value: model.DurationInline{Seconds: 20}}}
	}
	do := &model.TaskList{
		&model.TaskItem{Key: "race", Task: &model.ForkTask{
			Fork: model.ForkTaskConfiguration{
				Compete: true,
				Branches: &model.TaskList{
					&model.TaskItem{Key: "gamma", Task: newSetTask(map[string]any{testConstResult: testConstEnded})},
					&model.TaskItem{Key: "delta", Task: wait()},
					&model.TaskItem{Key: "epsilon", Task: wait()},
					&model.TaskItem{Key: "zeta", Task: wait()},
				},
			},
		}},
		&model.TaskItem{Key: testConstTaskAfter, Task: newSetTask(map[string]any{"done": true})},
	}

	fn := buildRootWorkflow(t, newRegistryMock(), doc, events, do)

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, nil)
	}, workflow.RegisterOptions{Name: replayForkWorkflowName})

	for range replayForkAttempts {
		require.NoError(t, replayer.ReplayWorkflowHistoryFromJSONFile(nil, "testdata/compete_fork_history.json"))
	}
}
