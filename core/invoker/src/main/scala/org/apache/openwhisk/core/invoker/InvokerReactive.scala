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

package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.nio.file.Files
import java.util.Base64
import scala.sys.process._
import spray.json._
import spray.json.DefaultJsonProtocol._

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorRefFactory, ActorSystem, CoordinatedShutdown, Props}
import org.apache.pekko.event.Logging.InfoLevel
import org.apache.openwhisk.common._
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.core.ack.{MessagingActiveAck, UserEventSender}
import org.apache.openwhisk.core.connector._
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.containerpool.logging.LogStoreProvider
import org.apache.openwhisk.core.containerpool.v2.{NotSupportedPoolState, TotalContainerPoolState}
import org.apache.openwhisk.core.database._
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.invoker.Invoker.InvokerEnabled
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import org.apache.openwhisk.spi.SpiLoader
import pureconfig._
import pureconfig.generic.auto._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object InvokerReactive extends InvokerProvider {
  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitsConfig: IntraConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new InvokerReactive(config, instance, producer, poolConfig, limitsConfig)
}

class InvokerReactive(
  config: WhiskConfig,
  instance: InvokerInstanceId,
  producer: MessageProducer,
  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
  limitsConfig: IntraConcurrencyLimitConfig = loadConfigOrThrow[IntraConcurrencyLimitConfig](
    ConfigKeys.concurrencyLimit))(implicit actorSystem: ActorSystem, logging: Logging)
    extends InvokerCore {

  implicit val ec: ExecutionContext = actorSystem.dispatcher
  implicit val cfg: WhiskConfig = config

  private val logsProvider = SpiLoader.get[LogStoreProvider].instance(actorSystem)
  logging.info(this, s"LogStoreProvider: ${logsProvider.getClass}")

  /**
   * Factory used by the ContainerProxy to physically create a new container.
   *
   * Create and initialize the container factory before kicking off any other
   * task or actor because further operation does not make sense if something
   * goes wrong here. Initialization will throw an exception upon failure.
   */
  private val containerFactory =
    SpiLoader
      .get[ContainerFactoryProvider]
      .instance(
        actorSystem,
        logging,
        config,
        instance,
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")) ++ logsProvider.containerParameters)
  containerFactory.init()

  CoordinatedShutdown(actorSystem)
    .addTask(CoordinatedShutdown.PhaseBeforeActorSystemTerminate, "cleanup runtime containers") { () =>
      containerFactory.cleanup()
      Future.successful(Done)
    }

  /** Initialize needed databases */
  private val entityStore = WhiskEntityStore.datastore()
  private val activationStore =
    SpiLoader.get[ActivationStoreProvider].instance(actorSystem, logging)

  private val authStore = WhiskAuthStore.datastore()

  private val namespaceBlacklist = new NamespaceBlacklist(authStore)

  Scheduler.scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) { () =>
    logging.debug(this, "running background job to update blacklist")
    namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
      case Success(set) => logging.info(this, s"updated blacklist to ${set.size} entries")
      case Failure(t)   => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
    }
  }

  /** Initialize message consumers */
  private val topic = s"${Invoker.topicPrefix}invoker${instance.toInt}"
  private val maximumContainers = (poolConfig.userMemory / MemoryLimit.MIN_MEMORY).toInt
  private val msgProvider = SpiLoader.get[MessagingProvider]

  //number of peeked messages - increasing the concurrentPeekFactor improves concurrent usage, but adds risk for message loss in case of crash
  private val maxPeek =
    math.max(maximumContainers, (maximumContainers * limitsConfig.max * poolConfig.concurrentPeekFactor).toInt)

  private val consumer =
    msgProvider.getConsumer(config, topic, topic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)

  private val activationFeed = actorSystem.actorOf(Props {
    new MessageFeed("activation", logging, consumer, maxPeek, 1.second, processActivationMessage)
  })

  private val ack = {
    val sender = if (UserEvents.enabled) Some(new UserEventSender(producer)) else None
    new MessagingActiveAck(producer, instance, sender)
  }

  private val collectLogs = new LogStoreCollector(logsProvider)

  /** Stores an activation in the database. */
  private val store = (tid: TransactionId, activation: WhiskActivation, isBlocking: Boolean, context: UserContext) => {
    implicit val transid: TransactionId = tid
    activationStore.storeAfterCheck(activation, isBlocking, None, None, context)(tid, notifier = None, logging)
  }

  /** Creates a ContainerProxy Actor when being called. */
  private val childFactory = (f: ActorRefFactory) =>
    f.actorOf(
      ContainerProxy
        .props(containerFactory.createContainer, ack, store, collectLogs, instance, poolConfig))

  val prewarmingConfigs: List[PrewarmingConfig] = {
    ExecManifest.runtimesManifest.stemcells.flatMap {
      case (mf, cells) =>
        cells.map { cell =>
          PrewarmingConfig(cell.initialCount, new CodeExecAsString(mf, "", None), cell.memory, cell.reactive)
        }
    }.toList
  }

  private val pool =
    actorSystem.actorOf(ContainerPool.props(childFactory, poolConfig, activationFeed, prewarmingConfigs))

  private def runConfidentialAction(action: WhiskAction, msg: ActivationMessage)(implicit transid: TransactionId): Future[Unit] = {
    Future {
      logging.info(this, s"Running confidential action ${action.fullyQualifiedName(false)}")

      // 1. Extract fields
      val fid = action.annotations.getAs[String]("FID").getOrElse("unknown")
      val cFuncBase64 = action.annotations.getAs[String]("C_func").getOrElse("")
      
      val content = msg.content.getOrElse(JsObject.empty).asJsObject
      val cReqBase64 = content.fields.get("C_req").map(_.convertTo[String]).getOrElse("")
      val pkUBase64 = content.fields.get("pkU").map(_.convertTo[String]).getOrElse("")
      val nonceBase64 = content.fields.get("nonce").map(_.convertTo[String]).getOrElse("")
      
      // 2. Write to temp files
      val tempDir = Files.createTempDirectory("sgx_exec")
      val wasmPath = tempDir.resolve("func.wasm.enc")
      val inputPath = tempDir.resolve("input.enc")
      val pkUPath = tempDir.resolve("pku.bin")
      val noncePath = tempDir.resolve("nonce.bin")
      
      Files.write(wasmPath, Base64.getDecoder.decode(cFuncBase64))
      Files.write(inputPath, Base64.getDecoder.decode(cReqBase64))
      Files.write(pkUPath, Base64.getDecoder.decode(pkUBase64))
      Files.write(noncePath, Base64.getDecoder.decode(nonceBase64))
      
      // 3. Run sgx_worker from its directory (required for enclave.signed.so)
      val workerDir = new java.io.File("/root/workspace/acsc/sgx_worker")
      val workerPath = "/root/workspace/acsc/sgx_worker/sgx_worker"
      val cmd = Seq(workerPath, "--invoke", fid, wasmPath.toString, inputPath.toString, pkUPath.toString, noncePath.toString)
      
      // Auto-detect SGX mode from .sgx_mode file
      val sgxModeFile = new java.io.File("/root/workspace/acsc/demo/.sgx_mode")
      val sgxMode = if (sgxModeFile.exists()) {
        scala.io.Source.fromFile(sgxModeFile).getLines().mkString.trim
      } else {
        "HW"  // Default to HW mode
      }
      
      // Set LD_LIBRARY_PATH based on SGX mode
      val ldLibPath = if (sgxMode == "HW") {
        "/usr/lib/x86_64-linux-gnu"
      } else {
        "/opt/intel/sgxsdk/lib64:/opt/intel/sgxsdk/sdk_libs"
      }
      val env = sys.env + ("LD_LIBRARY_PATH" -> ldLibPath)
      
      logging.info(this, s"SGX Mode: $sgxMode, LD_LIBRARY_PATH: $ldLibPath")
      
      val stdout = new StringBuilder
      val stderr = new StringBuilder
      val logger = ProcessLogger(
        (o: String) => stdout.append(o + "\n"),
        (e: String) => stderr.append(e + "\n")
      )
      
      val exitCode = Process(cmd, workerDir, env.toSeq: _*) ! logger
      
      logging.info(this, s"Exit code: $exitCode")
      logging.info(this, s"Stdout: $stdout")
      
      if (exitCode == 0) {
        // Parse output to find CT_HEX and RESULT_HEX
        val outputStr = stdout.toString()
        val ctHexPrefix = "CT_HEX:"
        val resultHexPrefix = "RESULT_HEX:"
        val ctLine = outputStr.linesIterator.find(_.startsWith(ctHexPrefix))
        val resultLine = outputStr.linesIterator.find(_.startsWith(resultHexPrefix))
        
        val resultJson = resultLine match {
          case Some(line) =>
            val hex = line.substring(resultHexPrefix.length)
            // Hex to Bytes
            val bytes = hex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
            val base64Result = Base64.getEncoder.encodeToString(bytes)

            val ctB64 = ctLine match {
              case Some(ct) =>
                val ctHex = ct.substring(ctHexPrefix.length)
                val ctBytes = ctHex.sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
                Base64.getEncoder.encodeToString(ctBytes)
              case None =>
                ""
            }
            
            JsObject(
              "C_out" -> JsString(base64Result),
              "ct" -> JsString(ctB64)
            )
          case None =>
            JsObject("error" -> JsString("No result from enclave"))
        }
        
        // Send Completion Message
        val activation = WhiskActivation(
          activationId = msg.activationId,
          namespace = msg.user.namespace.name.toPath,
          subject = msg.user.subject,
          cause = msg.cause,
          name = msg.action.name,
          version = msg.action.version.getOrElse(SemVer()),
          start = Instant.now(),
          end = Instant.now(),
          response = ActivationResponse.success(Some(resultJson)),
          logs = ActivationLogs(Vector(stdout.toString, stderr.toString)),
          duration = Some(100)
        )
        
        // Store activation and send completion
        val context = UserContext(msg.user)
        store(transid, activation, true, context)
        val completion = CombinedCompletionAndResultMessage(transid, activation, instance)
        producer.send(ConfigKeys.loadbalancer, completion)
      } else {
        logging.error(this, s"SGX Worker failed: $stderr")
        // Send Failure Message
         val activation = WhiskActivation(
          activationId = msg.activationId,
          namespace = msg.user.namespace.name.toPath,
          subject = msg.user.subject,
          cause = msg.cause,
          name = msg.action.name,
          version = msg.action.version.getOrElse(SemVer()),
          start = Instant.now(),
          end = Instant.now(),
          response = ActivationResponse.whiskError(s"SGX Worker failed: $stderr"),
          logs = ActivationLogs(Vector(stdout.toString, stderr.toString)),
          duration = Some(100)
        )
        // Store activation and send completion
        val context = UserContext(msg.user)
        store(transid, activation, true, context)
        val completion = CombinedCompletionAndResultMessage(transid, activation, instance)
        producer.send(ConfigKeys.loadbalancer, completion)
      }
      
      // Cleanup
      Files.deleteIfExists(wasmPath)
      Files.deleteIfExists(inputPath)
      Files.deleteIfExists(pkUPath)
      Files.deleteIfExists(noncePath)
      Files.deleteIfExists(tempDir)
    }
  }

  def handleActivationMessage(msg: ActivationMessage)(implicit transid: TransactionId): Future[Unit] = {
    val namespace = msg.action.path
    val name = msg.action.name
    val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
    val subject = msg.user.subject

    logging.debug(this, s"${actionid.id} $subject ${msg.activationId}")

    // caching is enabled since actions have revision id and an updated
    // action will not hit in the cache due to change in the revision id;
    // if the doc revision is missing, then bypass cache
    if (actionid.rev == DocRevision.empty) logging.warn(this, s"revision was not provided for ${actionid.id}")

    WhiskAction
      .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty)
      .flatMap(action => {
        // action that exceed the limit cannot be executed.
        action.limits.checkLimits(msg.user)
        
        if (action.annotations.isTruthy("confidential")) {
            runConfidentialAction(action, msg)
        } else {
            action.toExecutableWhiskAction match {
              case Some(executable) =>
                pool ! Run(executable, msg)
                Future.successful(())
              case None =>
                logging.error(this, s"non-executable action reached the invoker ${action.fullyQualifiedName(false)}")
                Future.failed(new IllegalStateException("non-executable action reached the invoker"))
            }
        }
      })
      .recoverWith {
        case DocumentRevisionMismatchException(_) =>
          // if revision is mismatched, the action may have been updated,
          // so try again with the latest code
          handleActivationMessage(msg.copy(revision = DocRevision.empty))
        case t =>
          val response = t match {
            case _: NoDocumentException =>
              ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
            case e: ActionLimitsException =>
              ActivationResponse.applicationError(e.getMessage) // return generated failed message
            case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
              ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
            case _ =>
              ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
          }
          activationFeed ! MessageFeed.Processed

          val activation = generateFallbackActivation(msg, response)
          ack(
            msg.transid,
            activation,
            msg.blocking,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          store(msg.transid, activation, msg.blocking, UserContext(msg.user))
          Future.successful(())
      }
  }

  /** Is called when an ActivationMessage is read from Kafka */
  def processActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg =>
        // The message has been parsed correctly, thus the following code needs to *always* produce at least an
        // active-ack.

        implicit val transid: TransactionId = msg.transid

        //set trace context to continue tracing
        WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

        if (!namespaceBlacklist.isBlacklisted(msg.user)) {
          val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
          handleActivationMessage(msg)
        } else {
          // Iff the current namespace is blacklisted, an active-ack is only produced to keep the loadbalancer protocol
          // Due to the protective nature of the blacklist, a database entry is not written.
          activationFeed ! MessageFeed.Processed

          val activation =
            generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))
          ack(
            msg.transid,
            activation,
            false,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked in invoker.")
          Future.successful(())
        }
      }
      .recoverWith {
        case t =>
          // Iff everything above failed, we have a terminal error at hand. Either the message failed
          // to deserialize, or something threw an error where it is not expected to throw.
          activationFeed ! MessageFeed.Processed
          logging.error(this, s"terminal failure while processing message: $t")
          Future.successful(())
      }
  }

  /**
   * Generates an activation with zero runtime. Usually used for error cases.
   *
   * Set the kind annotation to `Exec.UNKNOWN` since it is not known to the invoker because the action fetch failed.
   */
  private def generateFallbackActivation(msg: ActivationMessage, response: ActivationResponse): WhiskActivation = {
    val now = Instant.now
    val causedBy = if (msg.causedBySequence) {
      Some(Parameters(WhiskActivation.causedByAnnotation, JsString(Exec.SEQUENCE)))
    } else None

    WhiskActivation(
      activationId = msg.activationId,
      namespace = msg.user.namespace.name.toPath,
      subject = msg.user.subject,
      cause = msg.cause,
      name = msg.action.name,
      version = msg.action.version.getOrElse(SemVer()),
      start = now,
      end = now,
      duration = Some(0),
      response = response,
      annotations = {
        Parameters(WhiskActivation.pathAnnotation, JsString(msg.action.copy(version = None).asString)) ++
          Parameters(WhiskActivation.kindAnnotation, JsString(Exec.UNKNOWN)) ++ causedBy
      })
  }

  private val healthProducer = msgProvider.getProducer(config)

  private def getHealthScheduler: ActorRef =
    Scheduler.scheduleWaitAtMost(1.seconds)(() => pingController(isEnabled = true))

  private def pingController(isEnabled: Boolean) = {
    healthProducer.send(s"${Invoker.topicPrefix}health", PingMessage(instance, isEnabled = Some(isEnabled))).andThen {
      case Failure(t) => logging.error(this, s"failed to ping the controller: $t")
    }
  }

  private var healthScheduler: Option[ActorRef] = Some(getHealthScheduler)

  override def enable(): String = {
    if (healthScheduler.isEmpty) {
      healthScheduler = Some(getHealthScheduler)
      s"${instance.toString} is now enabled."
    } else {
      s"${instance.toString} is already enabled."
    }
  }

  override def disable(): String = {
    pingController(isEnabled = false)
    if (healthScheduler.nonEmpty) {
      actorSystem.stop(healthScheduler.get)
      healthScheduler = None
      s"${instance.toString} is now disabled."
    } else {
      s"${instance.toString} is already disabled."
    }
  }

  override def isEnabled(): String = {
    InvokerEnabled(healthScheduler.nonEmpty).serialize()
  }

  override def backfillPrewarm(): String = {
    "not supported"
  }

  override def getPoolState(): Future[Either[NotSupportedPoolState, TotalContainerPoolState]] = {
    Future.successful(Left(NotSupportedPoolState()))
  }
}
