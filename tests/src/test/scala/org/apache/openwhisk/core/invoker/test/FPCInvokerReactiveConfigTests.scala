/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */

package org.apache.openwhisk.core.invoker.test

import org.apache.openwhisk.core.invoker.FPCInvokerReactive
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FPCInvokerReactiveConfigTests extends AnyFlatSpec with Matchers {
  behavior of "Reusable FPC activation client inbound limit"

  it should "preserve the accepted eight MiB default" in {
    FPCInvokerReactive.reusableActivationMaxInboundMessageBytes(None) shouldBe 8 * 1024 * 1024
  }

  it should "accept the profile-local supplemental limit" in {
    FPCInvokerReactive.reusableActivationMaxInboundMessageBytes(Some("33554432")) shouldBe 32 * 1024 * 1024
  }

  it should "reject invalid and non-positive overrides" in {
    Seq("", "0", "-1", "not-a-number").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        FPCInvokerReactive.reusableActivationMaxInboundMessageBytes(Some(value))
      }
    }
  }
}
