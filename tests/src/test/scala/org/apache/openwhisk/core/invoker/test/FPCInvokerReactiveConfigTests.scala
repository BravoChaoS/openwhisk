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

package org.apache.openwhisk.core.invoker.test

import org.apache.openwhisk.core.invoker.FPCInvokerReactive
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FPCInvokerReactiveConfigTests extends AnyFlatSpec with Matchers {
  behavior of "FPC activation client inbound message limit"

  it should "preserve the gRPC default when the environment variable is absent" in {
    FPCInvokerReactive.activationMaxInboundMessageBytes(None) shouldBe None
  }

  it should "accept a positive byte limit" in {
    FPCInvokerReactive.activationMaxInboundMessageBytes(Some("16777216")) shouldBe Some(16777216)
  }

  it should "reject invalid or non-positive byte limits" in {
    Seq("", "0", "-1", "not-a-number").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        FPCInvokerReactive.activationMaxInboundMessageBytes(Some(value))
      }
    }
  }
}
