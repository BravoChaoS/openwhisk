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

package org.apache.openwhisk.core.scheduler.queue

import java.io.{EOFException, InputStream}
import java.net.{InetSocketAddress, Socket}
import java.nio.{ByteBuffer, ByteOrder}

import org.apache.pekko.util.ByteString

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{blocking, ExecutionContext, Future}
import scala.util.control.NonFatal

final case class GatewayControlConfig(enabled: Boolean,
                                      host: String,
                                      port: Int,
                                      connectTimeout: FiniteDuration,
                                      readTimeout: FiniteDuration,
                                      maxEnvelopeBytes: Int)

sealed abstract class GatewayControlError(val message: String)
final case class GatewayControlProtocolError(override val message: String) extends GatewayControlError(message)
final case class GatewayControlTransportError(override val message: String) extends GatewayControlError(message)
final case class GatewayControlRejected(status: Long)
    extends GatewayControlError(s"Gateway rejected re-encryption: status=$status")

trait GatewayControlClient {
  def reencryptToTarget(targetBindingId: Long,
                        sourceEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]]
}

/** Scheduler-local client for Gateway P1's Linux x86_64 FIFO-over-TCP control wire. */
final class P1GatewayControlClient(config: GatewayControlConfig)(implicit ec: ExecutionContext)
    extends GatewayControlClient {
  import P1GatewayControlClient._

  require(config.host.nonEmpty, "Gateway control host must not be empty")
  require(config.port > 0 && config.port <= 65535, "Gateway control port is invalid")
  require(config.connectTimeout.toMillis > 0, "Gateway connect timeout must be positive")
  require(config.readTimeout.toMillis > 0, "Gateway read timeout must be positive")
  require(config.maxEnvelopeBytes >= ProtectedEnvelopeV1.MinimumSize, "Gateway max envelope size is too small")

  override def reencryptToTarget(
    targetBindingId: Long,
    sourceEnvelope: ProtectedEnvelopeV1): Future[Either[GatewayControlError, ProtectedEnvelopeV1]] = Future {
    blocking {
      validateRequest(targetBindingId, sourceEnvelope).flatMap { _ =>
        try {
          Right(exchange(targetBindingId, sourceEnvelope))
        } catch {
          case error: GatewayControlException => Left(error.error)
          case NonFatal(t) =>
            Left(GatewayControlTransportError(s"Gateway control exchange failed: ${t.getClass.getSimpleName}"))
        }
      }
    }
  }

  private def validateRequest(targetBindingId: Long,
                              sourceEnvelope: ProtectedEnvelopeV1): Either[GatewayControlError, Unit] =
    for {
      _ <- Either.cond(targetBindingId > 0, (), GatewayControlProtocolError("target binding id must be positive"))
      _ <- Either.cond(
        sourceEnvelope.direction == ProtectedEnvelopeDirection.SourceToGateway,
        (),
        GatewayControlProtocolError("Gateway input envelope must use S2G direction"))
      _ <- Either.cond(
        sourceEnvelope.kind == ProtectedObjectKind.Code || sourceEnvelope.kind == ProtectedObjectKind.Input,
        (),
        GatewayControlProtocolError("Gateway to-target operation accepts only CODE or INPUT"))
      _ <- Either.cond(
        sourceEnvelope.bytes.length <= config.maxEnvelopeBytes,
        (),
        GatewayControlProtocolError("Gateway input envelope exceeds configured maximum"))
    } yield ()

  private def exchange(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1): ProtectedEnvelopeV1 = {
    val requestBody = encodeReencryptRequest(targetBindingId, sourceEnvelope)
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(config.host, config.port), timeoutMillis(config.connectTimeout))
      socket.setSoTimeout(timeoutMillis(config.readTimeout))
      val output = socket.getOutputStream
      output.write(encodeOuterFrame(ReencryptRequestType, requestBody.length).toArray)
      output.write(requestBody.toArray)
      output.flush()
      // P1 dispatches one FIFO request only after the client closes its write side.
      socket.shutdownOutput()

      val input = socket.getInputStream
      val responseHeader = decodeOuterHeader(readExactly(input, OuterHeaderSize))
      if (responseHeader.messageType != ReencryptResponseType) {
        fail(GatewayControlProtocolError(s"unexpected Gateway response type ${responseHeader.messageType}"))
      }
      if (responseHeader.bodySize < ReencryptResponsePrefixSize ||
          responseHeader.bodySize > ReencryptResponsePrefixSize.toLong + config.maxEnvelopeBytes) {
        fail(GatewayControlProtocolError(s"invalid Gateway response body size ${responseHeader.bodySize}"))
      }
      val body = readExactly(input, responseHeader.bodySize.toInt)
      decodeReencryptResponse(targetBindingId, sourceEnvelope, body)
    } finally {
      socket.close()
    }
  }

  private def encodeReencryptRequest(targetBindingId: Long, sourceEnvelope: ProtectedEnvelopeV1): ByteString = {
    val envelope = sourceEnvelope.bytes
    val output = ByteBuffer.allocate(ReencryptRequestPrefixSize + envelope.length).order(ByteOrder.BIG_ENDIAN)
    output.put(ControlVersion)
    output.put(ReencryptToTargetOperation)
    output.putShort(0)
    output.putLong(sourceEnvelope.bindingId)
    output.putLong(targetBindingId)
    output.putInt(envelope.length)
    output.put(envelope.toArray)
    ByteString.fromArray(output.array())
  }

  private def decodeReencryptResponse(targetBindingId: Long,
                                      sourceEnvelope: ProtectedEnvelopeV1,
                                      body: ByteString): ProtectedEnvelopeV1 = {
    val input = ByteBuffer.wrap(body.toArray).order(ByteOrder.BIG_ENDIAN)
    val version = input.get()
    val operation = input.get()
    val reserved = input.getShort()
    val status = input.getInt() & 0xffffffffL
    val envelopeLength = input.getInt()

    if (version != ControlVersion || operation != ReencryptToTargetOperation || reserved != 0) {
      fail(GatewayControlProtocolError("invalid Gateway re-encryption response prefix"))
    }
    if (status != 0) {
      fail(GatewayControlRejected(status))
    }
    if (envelopeLength < 0 || body.length != ReencryptResponsePrefixSize + envelopeLength) {
      fail(GatewayControlProtocolError("Gateway response envelope length mismatch"))
    }

    val targetEnvelope = ProtectedEnvelopeV1
      .decode(body.drop(ReencryptResponsePrefixSize))
      .fold(message => fail(GatewayControlProtocolError(s"invalid Gateway output envelope: $message")), identity)
    if (targetEnvelope.direction != ProtectedEnvelopeDirection.GatewayToTarget ||
        targetEnvelope.bindingId != targetBindingId ||
        targetEnvelope.kind != sourceEnvelope.kind ||
        targetEnvelope.correlationKind != sourceEnvelope.correlationKind ||
        targetEnvelope.correlationHash != sourceEnvelope.correlationHash) {
      fail(GatewayControlProtocolError("Gateway output envelope does not preserve target-bound correlation"))
    }
    targetEnvelope
  }

  private def timeoutMillis(duration: FiniteDuration): Int = math.min(duration.toMillis, Int.MaxValue.toLong).toInt
}

object P1GatewayControlClient {
  private val ControlVersion: Byte = 1
  private val ReencryptToTargetOperation: Byte = 1

  // FIFO_MSG_TYPE values from Gateway P1 Include/fifo_def.h.
  private[queue] val ReencryptRequestType = 11
  private[queue] val ReencryptResponseType = 12

  // Gateway P1 intentionally retains the native packed Linux x86_64 FIFO_MSG_HEADER.
  private[queue] val OuterHeaderSize = 16
  private val ReencryptRequestPrefixSize = 24
  private[queue] val ReencryptResponsePrefixSize = 12

  private final case class OuterHeader(messageType: Int, bodySize: Long)
  private final case class GatewayControlException(error: GatewayControlError) extends RuntimeException(error.message)

  private[queue] def encodeOuterFrame(messageType: Int, bodySize: Int): ByteString = {
    val output = ByteBuffer.allocate(OuterHeaderSize).order(ByteOrder.nativeOrder())
    output.putInt(messageType)
    output.putLong(bodySize.toLong)
    output.putInt(0)
    ByteString.fromArray(output.array())
  }

  private def decodeOuterHeader(bytes: ByteString): OuterHeader = {
    val input = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.nativeOrder())
    val messageType = input.getInt()
    val bodySize = input.getLong()
    input.getInt() // Gateway-side sockfd is transport-local and ignored.
    if (bodySize < 0) {
      fail(GatewayControlProtocolError("negative Gateway FIFO body size"))
    }
    OuterHeader(messageType, bodySize)
  }

  private[queue] def readExactly(input: InputStream, size: Int): ByteString = {
    val bytes = new Array[Byte](size)
    var offset = 0
    while (offset < size) {
      val count = input.read(bytes, offset, size - offset)
      if (count < 0) {
        throw new EOFException(s"Gateway control response ended after $offset of $size bytes")
      }
      offset += count
    }
    ByteString.fromArray(bytes)
  }

  private def fail(error: GatewayControlError): Nothing = throw GatewayControlException(error)
}
