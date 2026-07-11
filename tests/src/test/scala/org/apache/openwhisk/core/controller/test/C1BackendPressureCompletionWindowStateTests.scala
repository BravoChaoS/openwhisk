/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.controller.test

import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import org.apache.openwhisk.core.controller.C1BackendPressureCompletionWindowTransitions

@RunWith(classOf[JUnitRunner])
class C1BackendPressureCompletionWindowStateTests extends AnyFlatSpec with Matchers {

  behavior of "C1 backend-pressure completion-window transitions"

  it should "latch the first plateau stop while draining later completions" in {
    val targetLogicalRequests = 100000

    val beforeStop = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = 1230,
      remainingInFlight = 199,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = false,
      stopAlreadyLatched = false)

    beforeStop.refillLogicalRequestId shouldBe Some(1230)
    beforeStop.nextLogicalRequestId shouldBe 1231
    beforeStop.nextLogicalRequestId - 1 shouldBe 1230
    beforeStop.inFlightAfterCompletion shouldBe 200
    beforeStop.stopLatched shouldBe false

    val firstStop = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = 1230,
      remainingInFlight = 199,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = true,
      stopAlreadyLatched = false)

    firstStop.refillLogicalRequestId shouldBe None
    firstStop.nextLogicalRequestId - 1 shouldBe 1229
    firstStop.inFlightAfterCompletion shouldBe 199
    firstStop.stopLatched shouldBe true

    val drainedToSeven = (1 to 192).foldLeft(firstStop) { (state, _) =>
      C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
        nextLogicalRequestId = state.nextLogicalRequestId,
        remainingInFlight = state.inFlightAfterCompletion - 1,
        targetLogicalRequests = targetLogicalRequests,
        plateauStopNow = true,
        stopAlreadyLatched = state.stopLatched)
    }

    drainedToSeven.nextLogicalRequestId - 1 shouldBe 1229
    drainedToSeven.inFlightAfterCompletion shouldBe 7
    drainedToSeven.refillLogicalRequestId shouldBe None

    val laterQpsImprovement = C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
      nextLogicalRequestId = drainedToSeven.nextLogicalRequestId,
      remainingInFlight = drainedToSeven.inFlightAfterCompletion - 1,
      targetLogicalRequests = targetLogicalRequests,
      plateauStopNow = false,
      stopAlreadyLatched = drainedToSeven.stopLatched)

    laterQpsImprovement.nextLogicalRequestId - 1 shouldBe 1229
    laterQpsImprovement.inFlightAfterCompletion shouldBe 6
    laterQpsImprovement.refillLogicalRequestId shouldBe None
    laterQpsImprovement.stopLatched shouldBe true

    val fullyDrained = (1 to 6).foldLeft(laterQpsImprovement) { (state, _) =>
      C1BackendPressureCompletionWindowTransitions.afterTerminalCompletion(
        nextLogicalRequestId = state.nextLogicalRequestId,
        remainingInFlight = state.inFlightAfterCompletion - 1,
        targetLogicalRequests = targetLogicalRequests,
        plateauStopNow = false,
        stopAlreadyLatched = state.stopLatched)
    }

    fullyDrained.nextLogicalRequestId - 1 shouldBe 1229
    fullyDrained.inFlightAfterCompletion shouldBe 0
    fullyDrained.refillLogicalRequestId shouldBe None
    fullyDrained.stopLatched shouldBe true
  }
}
