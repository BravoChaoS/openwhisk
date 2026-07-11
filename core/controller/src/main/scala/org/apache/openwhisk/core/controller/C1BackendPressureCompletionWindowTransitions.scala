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

package org.apache.openwhisk.core.controller

private[controller] final case class C1BackendPressureCompletionWindowTransition(
  nextLogicalRequestId: Int,
  inFlightAfterCompletion: Int,
  refillLogicalRequestId: Option[Int],
  stopLatched: Boolean)

private[controller] object C1BackendPressureCompletionWindowTransitions {
  def afterTerminalCompletion(nextLogicalRequestId: Int,
                              remainingInFlight: Int,
                              targetLogicalRequests: Int,
                              plateauStopNow: Boolean,
                              stopAlreadyLatched: Boolean): C1BackendPressureCompletionWindowTransition = {
    val stopLatched = stopAlreadyLatched || plateauStopNow
    if (!stopLatched && nextLogicalRequestId <= targetLogicalRequests) {
      C1BackendPressureCompletionWindowTransition(
        nextLogicalRequestId = nextLogicalRequestId + 1,
        inFlightAfterCompletion = remainingInFlight + 1,
        refillLogicalRequestId = Some(nextLogicalRequestId),
        stopLatched = false)
    } else {
      C1BackendPressureCompletionWindowTransition(
        nextLogicalRequestId = nextLogicalRequestId,
        inFlightAfterCompletion = remainingInFlight,
        refillLogicalRequestId = None,
        stopLatched = stopLatched)
    }
  }
}
