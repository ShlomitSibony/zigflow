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

package tryexpressionerror

import (
	_ "embed"

	"github.com/zigflow/zigflow/tests/e2e/utils"
)

// testCase proves a failed runtime expression caught by a try reaches the catch
// with its message and its cause. The failure is a plain wrapped error rather
// than a Zigflow error, so the catch gets no RFC 7807 fields, but the error
// that was wrapped stays available as cause, as it did when the try body ran
// as a child workflow.
var testCase = utils.TestCase{
	Name:         "try-expression-error",
	WorkflowPath: "workflow.yaml",
	ExpectedOutput: map[string]any{
		"result": map[string]any{
			"fields": []any{"cause", "message"},
		},
	},
	Test: utils.RunToCompletion[map[string]any],
}

func init() {
	utils.AddTestCase(&testCase)
}
