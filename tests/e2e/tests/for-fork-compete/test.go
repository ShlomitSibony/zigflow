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

package forforkcompete

import (
	_ "embed"

	"github.com/zigflow/zigflow/tests/e2e/utils"
)

// testCase proves a competing fork can run in every iteration of a for loop.
// The iterations share the workflow's ID, so the fork reuses its branches'
// child workflow IDs each time. The fork cancels the losing branch when the
// winner replies and must wait for it to close: otherwise the next iteration
// fails with "child workflow execution already started".
var testCase = utils.TestCase{
	Name:         "for-fork-compete",
	WorkflowPath: "workflow.yaml",
	ExpectedOutput: map[string]any{
		"done": true,
	},
	Test: utils.RunToCompletion[map[string]any],
}

func init() {
	utils.AddTestCase(&testCase)
}
