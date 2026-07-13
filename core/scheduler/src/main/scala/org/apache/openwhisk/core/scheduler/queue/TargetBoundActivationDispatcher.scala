/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.util.Base64

import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.database.ArtifactStore
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.scheduler.grpc.GetActivation
import spray.json._

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** Same-fetch seam. H2 supplies the actual protected-code/input implementation. */
trait TargetBoundActivationDispatcher {
  def prepare(request: GetActivation,
              activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]]
}

object TargetBoundActivationDispatcher {

  /**
   * Preserve all existing profiles, but fail closed when a target-bound pull
   * reaches a scheduler that does not yet have the H2 dispatcher installed.
   */
  object Unconfigured extends TargetBoundActivationDispatcher {
    override def prepare(request: GetActivation,
                         activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]] =
      Future.successful(request.targetBindingId match {
        case None     => Right(activation)
        case Some(id) => Left(TargetReencryptionError(s"target-bound dispatcher unavailable for binding $id"))
      })
  }
}

final case class SourceProtectedCode(exactRevision: DocRevision, envelope: ProtectedEnvelopeV1)

trait ExactRevisionProtectedCodeProvider {
  def load(activation: ActivationMessage): Future[Either[TargetReencryptionError, SourceProtectedCode]]
}

object WhiskActionExactRevisionProtectedCodeProvider {
  type ActionLookup = (DocId, DocRevision, org.apache.openwhisk.common.TransactionId) => Future[WhiskAction]

  def apply(entityStore: ArtifactStore[WhiskEntity])(
    implicit ec: ExecutionContext): ExactRevisionProtectedCodeProvider = {
    val lookup: ActionLookup = (docId, revision, tid) => {
      implicit val transid = tid
      WhiskAction.get(entityStore, docId, revision, fromCache = false, ignoreMissingAttachment = false)
    }
    new WhiskActionExactRevisionProtectedCodeProvider(lookup)
  }
}

final class WhiskActionExactRevisionProtectedCodeProvider(
  lookup: WhiskActionExactRevisionProtectedCodeProvider.ActionLookup)(implicit ec: ExecutionContext)
    extends ExactRevisionProtectedCodeProvider {

  override def load(activation: ActivationMessage): Future[Either[TargetReencryptionError, SourceProtectedCode]] = {
    if (activation.revision == DocRevision.empty) {
      Future.successful(Left(TargetReencryptionError("target-bound code lookup requires an exact action revision")))
    } else {
      lookup(activation.action.toDocId, activation.revision, activation.transid)
        .map(extract(activation, _))
        .recover {
          case NonFatal(t) =>
            Left(TargetReencryptionError(s"exact-revision protected code lookup failed: ${t.getClass.getSimpleName}"))
        }
    }
  }

  private def extract(activation: ActivationMessage,
                      action: WhiskAction): Either[TargetReencryptionError, SourceProtectedCode] = {
    for {
      _ <- Either.cond(
        action.rev == activation.revision,
        (),
        TargetReencryptionError("exact-revision protected code lookup returned a different revision"))
      _ <- Either.cond(
        action.fullyQualifiedName(withVersion = false).toDocId == activation.action.toDocId,
        (),
        TargetReencryptionError("exact-revision protected code lookup returned a different action"))
      encoded <- action.exec match {
        case exec: CodeExec[_] if exec.binary =>
          exec.codeAsJson match {
            case JsString(value) => Right(value)
            case _               => Left(TargetReencryptionError("source-protected action code is not inline after lookup"))
          }
        case _: CodeExec[_] => Left(TargetReencryptionError("source-protected action code must be binary"))
        case _              => Left(TargetReencryptionError("target-bound action does not contain executable code"))
      }
      bytes <- TargetBoundActivationContent.decodeBase64(encoded, "source-protected action code")
      envelope <- ProtectedEnvelopeV1
        .decode(bytes)
        .left
        .map(message => TargetReencryptionError(s"invalid source-protected action code: $message"))
      _ <- TargetBoundActivationContent.requireEnvelope(
        envelope,
        ProtectedObjectKind.Code,
        ProtectedCorrelationKind.CodeObject,
        "source-protected action code")
    } yield SourceProtectedCode(activation.revision, envelope)
  }
}

final class GatewayTargetBoundActivationDispatcher(codeProvider: ExactRevisionProtectedCodeProvider,
                                                   gatewayClient: GatewayControlClient)(implicit ec: ExecutionContext)
    extends TargetBoundActivationDispatcher {

  override def prepare(request: GetActivation,
                       activation: ActivationMessage): Future[Either[TargetReencryptionError, ActivationMessage]] =
    request.targetBindingId match {
      case None => Future.successful(Right(activation))
      case Some(targetBindingId) if targetBindingId <= 0 =>
        Future.successful(Left(TargetReencryptionError("target binding id must be positive")))
      case Some(targetBindingId) => prepareTargetBound(request, activation, targetBindingId)
    }

  private def prepareTargetBound(request: GetActivation,
                                 activation: ActivationMessage,
                                 targetBindingId: Long): Future[Either[TargetReencryptionError, ActivationMessage]] = {
    if (activation.revision == DocRevision.empty) {
      Future.successful(Left(TargetReencryptionError("target-bound dispatch requires an exact action revision")))
    } else
      TargetBoundActivationContent.sourceInput(activation.content) match {
        case Left(error) => Future.successful(Left(error))
        case Right(sourceInput) if request.warmed =>
          reencrypt(targetBindingId, sourceInput).map(
            _.map(
              targetInput =>
                withTargetDispatch(
                  activation,
                  targetBindingId,
                  sourceInput.bindingId,
                  activation.revision,
                  warmed = true,
                  targetInput,
                  None)))
        case Right(sourceInput) =>
          codeProvider.load(activation).flatMap {
            case Left(error) => Future.successful(Left(error))
            case Right(sourceCode) =>
              reencrypt(targetBindingId, sourceCode.envelope).flatMap {
                case Left(error) => Future.successful(Left(error))
                case Right(targetCode) =>
                  reencrypt(targetBindingId, sourceInput).map(
                    _.map(
                      targetInput =>
                        withTargetDispatch(
                          activation,
                          targetBindingId,
                          sourceInput.bindingId,
                          sourceCode.exactRevision,
                          warmed = false,
                          targetInput,
                          Some(targetCode))))
              }
          }
      }
  }

  private def withTargetDispatch(activation: ActivationMessage,
                                 targetBindingId: Long,
                                 sourceBindingId: Long,
                                 exactRevision: DocRevision,
                                 warmed: Boolean,
                                 targetInput: ProtectedEnvelopeV1,
                                 targetCode: Option[ProtectedEnvelopeV1]): ActivationMessage =
    activation.copy(
      content = Some(
        TargetBoundActivationContent
          .targetDispatch(targetBindingId, sourceBindingId, exactRevision, warmed, targetInput, targetCode)))

  private def reencrypt(
    targetBindingId: Long,
    sourceEnvelope: ProtectedEnvelopeV1): Future[Either[TargetReencryptionError, ProtectedEnvelopeV1]] =
    gatewayClient
      .reencryptToTarget(targetBindingId, sourceEnvelope)
      .map(_.left.map(error => TargetReencryptionError(error.message)))
      .recover {
        case NonFatal(t) =>
          Left(TargetReencryptionError(s"Gateway re-encryption failed: ${t.getClass.getSimpleName}"))
      }
}

/** JSON contract carried by ActivationMessage before and after same-fetch dispatch. */
object TargetBoundActivationContent {
  val RootField = "__ow_reusable_target_bound_v1"
  val SourceInputField = "sourceProtectedInputEnvelope"
  val TargetInputField = "targetProtectedInputEnvelope"
  val TargetCodeField = "targetProtectedCodeEnvelope"
  val TargetBindingField = "targetBindingId"
  val SourceBindingField = "sourceBindingId"
  val ExactRevisionField = "exactActionRevision"
  val WarmedField = "warmed"
  val WorkerCodeFetchField = "workerCodeFetch"

  def sourceInput(content: Option[JsValue]): Either[TargetReencryptionError, ProtectedEnvelopeV1] =
    for {
      root <- content match {
        case Some(JsObject(fields)) if fields.keySet == Set(RootField) =>
          fields(RootField) match {
            case value: JsObject => Right(value)
            case _               => Left(TargetReencryptionError("target-bound source content root must be an object"))
          }
        case _ =>
          Left(TargetReencryptionError("target-bound activation must contain only the protected input contract"))
      }
      encoded <- root.fields.get(SourceInputField) match {
        case Some(JsString(value)) if root.fields.keySet == Set(SourceInputField) => Right(value)
        case _                                                                    => Left(TargetReencryptionError("target-bound activation is missing its source-protected INPUT"))
      }
      bytes <- decodeBase64(encoded, "source-protected INPUT")
      envelope <- ProtectedEnvelopeV1
        .decode(bytes)
        .left
        .map(message => TargetReencryptionError(s"invalid source-protected INPUT: $message"))
      _ <- requireEnvelope(
        envelope,
        ProtectedObjectKind.Input,
        ProtectedCorrelationKind.RequestId,
        "source-protected INPUT")
    } yield envelope

  def targetDispatch(targetBindingId: Long,
                     sourceBindingId: Long,
                     exactRevision: DocRevision,
                     warmed: Boolean,
                     targetInput: ProtectedEnvelopeV1,
                     targetCode: Option[ProtectedEnvelopeV1]): JsObject = {
    val fields = Map[String, JsValue](
      TargetBindingField -> JsString(targetBindingId.toString),
      SourceBindingField -> JsString(sourceBindingId.toString),
      ExactRevisionField -> JsString(exactRevision.rev),
      WarmedField -> JsBoolean(warmed),
      WorkerCodeFetchField -> JsBoolean(false),
      TargetInputField -> JsString(encodeBase64(targetInput.bytes))) ++ targetCode.map { code =>
      TargetCodeField -> JsString(encodeBase64(code.bytes))
    }
    JsObject(RootField -> JsObject(fields))
  }

  private[queue] def requireEnvelope(envelope: ProtectedEnvelopeV1,
                                     kind: ProtectedObjectKind,
                                     correlationKind: ProtectedCorrelationKind,
                                     description: String): Either[TargetReencryptionError, Unit] =
    for {
      _ <- Either.cond(
        envelope.direction == ProtectedEnvelopeDirection.SourceToGateway,
        (),
        TargetReencryptionError(s"$description must use S2G direction"))
      _ <- Either.cond(envelope.kind == kind, (), TargetReencryptionError(s"$description has the wrong object kind"))
      _ <- Either.cond(
        envelope.correlationKind == correlationKind,
        (),
        TargetReencryptionError(s"$description has the wrong correlation kind"))
    } yield ()

  private[queue] def decodeBase64(
    value: String,
    description: String): Either[TargetReencryptionError, org.apache.pekko.util.ByteString] =
    try {
      Right(org.apache.pekko.util.ByteString.fromArray(Base64.getDecoder.decode(value)))
    } catch {
      case _: IllegalArgumentException => Left(TargetReencryptionError(s"$description is not valid base64"))
    }

  private[queue] def encodeBase64(bytes: org.apache.pekko.util.ByteString): String =
    Base64.getEncoder.encodeToString(bytes.toArray)
}
