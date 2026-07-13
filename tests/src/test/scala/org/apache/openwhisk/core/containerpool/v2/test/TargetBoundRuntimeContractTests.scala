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

package org.apache.openwhisk.core.containerpool.v2.test

import java.util.Base64

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.ByteString
import com.typesafe.config.ConfigFactory
import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}
import org.apache.openwhisk.core.containerpool.v2._
import org.apache.openwhisk.core.entity.{ActivationResponse, DocRevision}
import org.apache.openwhisk.core.scheduler.queue._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class TargetBoundRuntimeContractTests
    extends TestKit(
      ActorSystem(
        "TargetBoundRuntimeContract",
        ConfigFactory.parseString("pekko.actor.provider=local").withFallback(ConfigFactory.load())))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit private val ec = system.dispatcher

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  behavior of "GatewayTargetBindingProvider"

  it should "register ACTIVE, write the binding into the same executor, and close it" in {
    @volatile var activated = Option.empty[Long]
    val lifecycle = ArrayBuffer.empty[String]
    val bridge = new TargetEndpointBridgeClient {
      override def bind(containerIdentity: String, targetHost: String, targetPort: Int) = {
        containerIdentity shouldBe "container-1"
        targetHost shouldBe "10.0.0.2"
        targetPort shouldBe 9200
        lifecycle += "bridge-put"
        Future.successful(())
      }
      override def unbind(containerIdentity: String) = {
        containerIdentity shouldBe "container-1"
        lifecycle += "bridge-delete"
        Future.successful(())
      }
    }
    val gateway = new GatewayControlClient {
      override def registerTarget(containerIdentity: String, endpointHost: String, endpointPort: Int) = {
        lifecycle += "gateway-register"
        containerIdentity shouldBe "container-1"
        endpointHost shouldBe "172.18.89.215"
        endpointPort shouldBe 9200
        Future.successful(Right(GatewayRegisteredTarget(41, 7)))
      }
      override def closeTarget(targetBindingId: Long) = {
        targetBindingId shouldBe 41
        lifecycle += "gateway-close"
        Future.successful(Right(()))
      }
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
    }
    val provider =
      new GatewayTargetBindingProvider(gateway, bridge, "172.18.89.215", 9200, 9200, 2.seconds, 10.millis)
    val target = TargetContainer(
      ContainerId("container-1"),
      ContainerAddress("10.0.0.2"),
      TargetBindingProvider.ReusableConcurrencyKind,
      (bindingId, _) => {
        lifecycle += "executor-target"
        activated = Some(bindingId)
        Future.successful(())
      })

    val binding = Await.result(provider.awaitReady(target), 3.seconds)
    binding.id shouldBe 41
    activated shouldBe Some(41)
    Await.result(provider.closeBinding(target, binding), 3.seconds)
    lifecycle shouldBe Seq("bridge-put", "gateway-register", "executor-target", "gateway-close", "bridge-delete")
  }

  it should "close a Gateway registration when the executor rejects /target" in {
    val lifecycle = ArrayBuffer.empty[String]
    val bridge = new TargetEndpointBridgeClient {
      override def bind(containerIdentity: String, targetHost: String, targetPort: Int) = {
        lifecycle += "bridge-put"
        Future.successful(())
      }
      override def unbind(containerIdentity: String) = {
        lifecycle += "bridge-delete"
        Future.successful(())
      }
    }
    val gateway = new GatewayControlClient {
      override def registerTarget(containerIdentity: String, endpointHost: String, endpointPort: Int) = {
        lifecycle += "gateway-register"
        Future.successful(Right(GatewayRegisteredTarget(42, 8)))
      }
      override def closeTarget(targetBindingId: Long) = {
        lifecycle += "gateway-close"
        Future.successful(Right(()))
      }
      override def reencryptToTarget(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1) =
        Future.successful(Left(GatewayControlProtocolError("unused")))
    }
    val provider =
      new GatewayTargetBindingProvider(gateway, bridge, "172.18.89.215", 9200, 9200, 2.seconds, 10.millis)
    val target = TargetContainer(
      ContainerId("container-2"),
      ContainerAddress("10.0.0.3"),
      TargetBindingProvider.ReusableConcurrencyKind,
      (_, _) => {
        lifecycle += "executor-target"
        Future.failed(new IllegalStateException("executor rejected target"))
      })

    intercept[IllegalStateException](Await.result(provider.awaitReady(target), 3.seconds))
    lifecycle shouldBe Seq("bridge-put", "gateway-register", "executor-target", "gateway-close", "bridge-delete")
  }

  behavior of "FunctionPullingContainerProxy target-bound result adapter"

  it should "preserve source binding and T2G request correlation in the full ack result" in {
    val dispatch = TargetBoundActivationContent.TargetDispatch(
      targetBindingId = 41,
      sourceBindingId = 17,
      DocRevision("1-exact"),
      warmed = true,
      targetInput,
      targetCode = None)
    val response = ActivationResponse.success(
      Some(
        JsObject(
          "status" -> JsString("success"),
          "status_code" -> JsNumber(0),
          "success" -> JsBoolean(true),
          "result" -> JsObject(
            "__reusable_protected_result_envelope" -> JsString(
              Base64.getEncoder.encodeToString(targetResult.bytes.toArray)),
            "__reusable_protected_result_len" -> JsNumber(64),
            "__reusable_request_id_hash" -> JsString(requestHash),
            "__reusable_target_binding_id" -> JsNumber(41)))))

    val adapted = FunctionPullingContainerProxy.targetBoundRuntimeResponse(dispatch, response)
    adapted.isSuccess shouldBe true
    val result = TargetBoundActivationContent.parseTargetResult(adapted.result).toOption.get
    result.sourceBindingId shouldBe 17
    result.targetBindingId shouldBe 41
    result.requestIdHash shouldBe requestHash
    result.targetEnvelope shouldBe targetResult
  }

  private val correlation = ByteString.fromArray((0 until 32).map(_.toByte).toArray)
  private val requestHash = correlation.map(byte => f"${byte & 0xff}%02x").mkString
  private val targetInput = ProtectedEnvelopeV1(
    ProtectedObjectKind.Input,
    ProtectedEnvelopeDirection.GatewayToTarget,
    41,
    ProtectedCorrelationKind.RequestId,
    correlation,
    ByteString.fromArray((0 until 12).map(_.toByte).toArray),
    ByteString("input"),
    ByteString.fromArray(Array.fill(16)(1.toByte)))
  private val targetResult = ProtectedEnvelopeV1(
    ProtectedObjectKind.Result,
    ProtectedEnvelopeDirection.TargetToGateway,
    41,
    ProtectedCorrelationKind.RequestId,
    correlation,
    ByteString.fromArray((128 until 140).map(_.toByte).toArray),
    ByteString("result"),
    ByteString.fromArray(Array.fill(16)(2.toByte)))
}
