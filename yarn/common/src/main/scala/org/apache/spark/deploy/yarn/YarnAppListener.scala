/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import org.apache.hadoop.yarn.api.records.ApplicationId

sealed trait YarnApplicationEvent

case class YarnApplicationStart(time: Long) extends YarnApplicationEvent
case class YarnApplicationProgress(time: Long, progress: YarnAppProgress) extends YarnApplicationEvent
case class YarnApplicationEnd(time: Long) extends YarnApplicationEvent

trait YarnApplicationListener {
  def onApplicationInit(time:Long, appId: ApplicationId)
  def onApplicationStart(time:Long, info: YarnAppInfo)
  def onApplicationProgress(time:Long, progress: YarnAppProgress)
  def onApplicationEnd(time:Long, progress: YarnAppProgress)
  def onApplicationFailed(time:Long, progress: YarnAppProgress)
  def onApplicationKilled(time:Long, progress: YarnAppProgress)

}
