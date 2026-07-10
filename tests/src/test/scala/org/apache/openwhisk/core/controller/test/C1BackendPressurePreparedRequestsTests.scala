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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import scala.collection.JavaConverters._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner
import spray.json._
import org.apache.openwhisk.core.controller.C1BackendPressurePreparedRequests
import org.apache.openwhisk.core.entity.ExecManifest.ImageName
import org.apache.openwhisk.core.entity._

@RunWith(classOf[JUnitRunner])
class C1BackendPressurePreparedRequestsTests extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private var root: Path = _

  override protected def beforeEach(): Unit = {
    root = Files.createTempDirectory("c1-asyncs-prepared-requests-")
  }

  override protected def afterEach(): Unit = {
    if (root != null) {
      Files.walk(root).iterator().asScala.toVector.reverse.foreach(Files.deleteIfExists)
    }
  }

  private def sha256(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map(byte => f"${byte & 0xff}%02x").mkString

  private def actionMetaData(): WhiskActionMetaData =
    WhiskActionMetaData(
      EntityPath("fixture-namespace/fixture-package"),
      EntityName("asyncs_backend_pressure_fixture"),
      BlackBoxExecMetaData(ImageName("fixture.registry/acsc/openwhisk-runtime:fixture"), None, native = false),
      version = SemVer(1, 2, 3),
      binding = Some(EntityPath("fixture-namespace/source-package")))
      .revision[WhiskActionMetaData](DocRevision("7-fixture-revision"))

  private def writeSource(action: WhiskActionMetaData, count: Int = 3): Unit = {
    val rows = (1 to count).map { ordinal =>
      JsObject(
        "action_params" -> JsObject(
          "logical_request_id" -> JsString(ordinal.toString),
          "attempt_id" -> JsString("1"),
          "encrypted_input" -> JsString(s"ciphertext-$ordinal")),
        "expected_rid" -> JsString(s"rid-$ordinal"),
        "logical_request_id" -> JsString(ordinal.toString),
        "ordinal" -> JsNumber(ordinal)).compactPrint
    }
    val requestBytes = (rows.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
    Files.write(root.resolve("requests.jsonl"), requestBytes)
    val identity = C1BackendPressurePreparedRequests.actionIdentity(action)
    val manifest = JsObject(
      "action" -> JsObject(
        "canonical_fqen" -> identity.canonicalFqen,
        "requested_action" -> JsString("fixture-provenance-only"),
        "revision" -> JsString(identity.revision)),
      "configured_request_count" -> JsNumber(count),
      "failure_probability" -> JsNumber(0),
      "requests_file" -> JsString("requests.jsonl"),
      "requests_sha256" -> JsString(sha256(requestBytes)),
      "schema_version" -> JsString("c1-asyncs-premeasurement-requests-v1"),
      "workload" -> JsObject("workload_id" -> JsString("asyncs-wasm-gzip-level6-mixed-512k")))
    Files.write(root.resolve("manifest.json"), (manifest.prettyPrint + "\n").getBytes(StandardCharsets.UTF_8))
  }

  behavior of "C1 backend-pressure prepared request source"

  it should "load sequential action parameters bound to canonical action metadata" in {
    val action = actionMetaData()
    writeSource(action)

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedAction = C1BackendPressurePreparedRequests.actionIdentity(action),
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.isRight shouldBe true
    val requests = result.right.get
    requests.map(_.ordinal) shouldBe Vector(1, 2, 3)
    requests.map(_.logicalRequestId) shouldBe Vector("1", "2", "3")
    requests.map(_.expectedRid) shouldBe Vector("rid-1", "rid-2", "rid-3")
    requests(1).actionParams.fields("encrypted_input") shouldBe JsString("ciphertext-2")
  }

  it should "reject a source bound to a different canonical revision" in {
    val action = actionMetaData()
    writeSource(action)
    val differentRevision = C1BackendPressurePreparedRequests
      .actionIdentity(action)
      .copy(revision = "8-different-revision")

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedAction = differentRevision,
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.left.get should include("revision")
  }

  it should "reject a source bound to a different canonical FQEN" in {
    val action = actionMetaData()
    writeSource(action)
    val identity = C1BackendPressurePreparedRequests.actionIdentity(action)
    val differentFqen = identity.copy(
      canonicalFqen = JsObject(identity.canonicalFqen.fields + ("name" -> JsString("different-action"))))

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedAction = differentFqen,
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.left.get should include("canonical_fqen")
  }

  it should "reject reordered rows even when their file hash matches the manifest" in {
    val action = actionMetaData()
    writeSource(action)
    val requestsPath = root.resolve("requests.jsonl")
    val reordered = Files.readAllLines(requestsPath, StandardCharsets.UTF_8).asScala.reverse.mkString("\n") + "\n"
    Files.write(requestsPath, reordered.getBytes(StandardCharsets.UTF_8))
    val manifestPath = root.resolve("manifest.json")
    val manifest = new String(Files.readAllBytes(manifestPath), StandardCharsets.UTF_8).parseJson.asJsObject
    val updated = JsObject(manifest.fields + ("requests_sha256" -> JsString(sha256(reordered.getBytes(StandardCharsets.UTF_8)))))
    Files.write(manifestPath, (updated.prettyPrint + "\n").getBytes(StandardCharsets.UTF_8))

    val result = C1BackendPressurePreparedRequests.load(
      root.toString,
      expectedCount = 3,
      expectedAction = C1BackendPressurePreparedRequests.actionIdentity(action),
      expectedWorkloadId = "asyncs-wasm-gzip-level6-mixed-512k")

    result.left.get should include("out of order")
  }
}
