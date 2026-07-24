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

import com.typesafe.config.ConfigFactory
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.apache.openwhisk.core.invoker.FPCInvokerReactive
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.grpc.GrpcClientSettings
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class FPCInvokerReactiveConfigTests extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val DefaultGrpcMaxInboundMessageBytes = 4 * 1024 * 1024
  private val NativeE3MaxInboundMessageBytes = 8 * 1024 * 1024
  private implicit val actorSystem: ActorSystem =
    ActorSystem(
      "fpc-inbound-limit-test",
      ConfigFactory.parseString("pekko.actor.provider = local").withFallback(ConfigFactory.load()))

  override def afterAll(): Unit = {
    Await.result(actorSystem.terminate(), 30.seconds)
    super.afterAll()
  }

  private def configuredMaxInboundMessageBytes(settings: GrpcClientSettings): Int = {
    val builder = NettyChannelBuilder.forAddress("127.0.0.1", 12345)
    val configured = settings.channelBuilderOverrides.apply(builder)
    val field = classOf[NettyChannelBuilder].getDeclaredField("maxInboundMessageSize")
    field.setAccessible(true)
    field.getInt(configured)
  }

  behavior of "FPC activation client inbound message limit"

  it should "preserve the gRPC default when the environment variable is absent" in {
    FPCInvokerReactive.activationMaxInboundMessageBytes(None) shouldBe None

    Seq("original-scheduler", "fallback-scheduler").foreach { host =>
      val settings = FPCInvokerReactive.activationClientSettings(host, 12345, tls = false, None)
      configuredMaxInboundMessageBytes(settings) shouldBe DefaultGrpcMaxInboundMessageBytes
    }
  }

  it should "apply 8 MiB to original and fallback scheduler activation clients" in {
    FPCInvokerReactive.activationMaxInboundMessageBytes(Some("8388608")) shouldBe
      Some(NativeE3MaxInboundMessageBytes)

    Seq("original-scheduler", "fallback-scheduler").foreach { host =>
      val settings =
        FPCInvokerReactive.activationClientSettings(
          host,
          12345,
          tls = false,
          Some(NativeE3MaxInboundMessageBytes))
      configuredMaxInboundMessageBytes(settings) shouldBe NativeE3MaxInboundMessageBytes
    }
  }

  it should "reject invalid or non-positive byte limits" in {
    Seq("", "0", "-1", "not-a-number").foreach { value =>
      an[IllegalArgumentException] should be thrownBy {
        FPCInvokerReactive.activationMaxInboundMessageBytes(Some(value))
      }
    }
  }
}
