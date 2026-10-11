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

package inlinefortry

import (
	"context"
	"testing"

	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporal "github.com/zigflow/helpers"
	"github.com/zigflow/zigflow/tests/e2e/utils"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
)

// caughtKey is the key the catch body sets, as in workflow.yaml.
const caughtKey = "caught"

// testCase proves for iterations and try/catch bodies run inline in the
// workflow that contains them. Every iteration fails inside a try body and is
// caught, so the output shows the loop carried on, and the history must hold no
// child workflow events.
var testCase = utils.TestCase{
	Name:         "inline-for-try",
	WorkflowPath: "workflow.yaml",
	Test: func(t *testing.T, test *utils.TestCase) {
		c, err := temporal.NewConnectionWithEnvvars(temporal.WithZerolog(&zlog.Logger))
		require.NoError(t, err)
		defer c.Close()

		ctx := context.Background()
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue: test.Workflow.Document.Namespace,
		}, test.Workflow.Document.Name, test.Input)
		require.NoError(t, err)

		var output []any
		require.NoError(t, run.Get(ctx, &output))
		assert.Equal(t, []any{
			map[string]any{caughtKey: float64(1)},
			map[string]any{caughtKey: float64(2)},
			map[string]any{caughtKey: float64(3)},
		}, output)

		assert.Zero(t, childWorkflowsStarted(t, c, run.GetID(), run.GetRunID()))
	},
}

func childWorkflowsStarted(t *testing.T, c client.Client, workflowID, runID string) int {
	t.Helper()

	started := 0
	iter := c.GetWorkflowHistory(context.Background(), workflowID, runID, false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err)
		if event.GetEventType() == enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED {
			started++
		}
	}

	return started
}

func init() {
	utils.AddTestCase(&testCase)
}
