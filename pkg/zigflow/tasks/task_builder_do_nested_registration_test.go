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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func setTask(key string, value map[string]any) *model.TaskItem {
	return &model.TaskItem{Key: key, Task: &model.SetTask{Set: model.NewObjectOrRuntimeExpr(value)}}
}

func endingDoTask(key string) *model.TaskItem {
	end := setTask("finish", map[string]any{"result": "ended"})
	end.Task.(*model.SetTask).Then = &model.FlowDirective{Value: string(model.FlowDirectiveEnd)}
	return &model.TaskItem{Key: key, Task: &model.DoTask{Do: &model.TaskList{end}}}
}

// A do task that a switch in an inline body can redirect to must stay
// registered as a workflow, and run as one: `then: end` inside it must reach
// the caller as an end signal, not as a nested flow.ErrEnd failure.
func TestNestedBodyRegistersDoTasksAsStandaloneWorkflows(t *testing.T) {
	tests := map[string]*model.TaskList{
		"inline do before the first non-do task": {endingDoTask("target"), setTask("step", map[string]any{"a": 1})},
		"redirect-only do after a non-do task":   {setTask("step", map[string]any{"a": 1}), endingDoTask("target")},
	}

	for name, list := range tests {
		t.Run(name, func(t *testing.T) {
			var registered TemporalWorkflowFunc
			temporalWorker := new(WorkflowRegistryMock)
			temporalWorker.
				On("RegisterWorkflowWithOptions", mock.Anything, workflow.RegisterOptions{Name: "target"}).
				Run(func(args mock.Arguments) { registered = args.Get(0).(TemporalWorkflowFunc) }).
				Once()

			builder, err := NewDoTaskBuilder(temporalWorker, &model.DoTask{Do: list}, "", testWorkflow, testEvents, nil,
				DoTaskOpts{DisableRegisterWorkflow: true, Nested: true})
			require.NoError(t, err)
			_, err = builder.Build()
			require.NoError(t, err)
			temporalWorker.AssertExpectations(t)
			require.NotNil(t, registered)

			var s testsuite.WorkflowTestSuite
			env := s.NewTestWorkflowEnvironment()
			env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
				return registered(ctx, nil, utils.NewState())
			}, workflow.RegisterOptions{Name: "target"})
			env.ExecuteWorkflow("target")

			require.True(t, env.IsWorkflowCompleted())
			assert.NoError(t, env.GetWorkflowError())
		})
	}
}
