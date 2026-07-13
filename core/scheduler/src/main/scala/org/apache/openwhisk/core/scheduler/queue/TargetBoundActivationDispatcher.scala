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

import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.scheduler.grpc.GetActivation

import scala.concurrent.Future

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
