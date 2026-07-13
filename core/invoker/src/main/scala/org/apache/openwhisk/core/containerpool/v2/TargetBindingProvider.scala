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

package org.apache.openwhisk.core.containerpool.v2

import org.apache.openwhisk.core.containerpool.{ContainerAddress, ContainerId}

import scala.concurrent.Future

/** Opaque control-side handle for one ACTIVE container target. */
final case class TargetBinding(id: Long) {
  require(id > 0, "target binding id must be positive")
}

/** Concrete container identity supplied to a profile-specific binding provider. */
final case class TargetContainer(containerId: ContainerId, address: ContainerAddress, kind: String)

/**
 * Lifecycle seam implemented by profiles that require target-bound dispatch.
 * A provider must not complete awaitReady until its real runtime session is ACTIVE.
 */
trait TargetBindingProvider {
  def requiresBinding(kind: String): Boolean
  def awaitReady(target: TargetContainer): Future[TargetBinding]
  def closeBinding(target: TargetContainer, binding: TargetBinding): Future[Unit]
}

object TargetBindingProvider {

  /** Existing profiles explicitly have no target-binding lifecycle. */
  object Disabled extends TargetBindingProvider {
    override def requiresBinding(kind: String): Boolean = false

    override def awaitReady(target: TargetContainer): Future[TargetBinding] =
      Future.failed(new IllegalStateException("target binding provider is disabled"))

    override def closeBinding(target: TargetContainer, binding: TargetBinding): Future[Unit] =
      Future.failed(new IllegalStateException("target binding provider is disabled"))
  }
}
