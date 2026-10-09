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

package trydoend

import (
	_ "embed"

	"github.com/zigflow/zigflow/tests/e2e/utils"
)

// testCase proves `then: end` inside a `do` task nested in a try body ends the
// whole workflow. The inner task list shares the workflow's Temporal context,
// so it must be built as nested like the try body, or it consumes the end
// directive and the tasks after it keep running. `catchRan`, `inTry` and
// `afterTry` must all be absent.
var testCase = utils.TestCase{
	Name:         "try-do-end",
	WorkflowPath: "workflow.yaml",
	ExpectedOutput: map[string]any{
		"result": "ended",
	},
	Test: utils.RunToCompletion[map[string]any],
}

func init() {
	utils.AddTestCase(&testCase)
}
