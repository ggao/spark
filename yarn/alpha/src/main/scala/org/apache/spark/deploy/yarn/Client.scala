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

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Records, Apps}
import org.apache.spark.deploy.yarn.{YarnAppProgress, ClientArguments, YarnResourceCapacity, ClientBase}

import org.apache.spark.{SparkConf, Logging}

/**
 * Version of [[org.apache.spark.deploy.yarn.ClientBase]] tailored to YARN's alpha API.
 */
class Client( hadoopConf: Configuration)
  extends YarnClientImpl with ClientBase with Logging {

  def this() = this(new Configuration())
  val conf = hadoopConf
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  def createYarnApplication() : GetNewApplicationResponse = {
    init(yarnConf)
    start()
    val newApp = super.getNewApplication
    newApp
  }

  def run(f: YarnResourceCapacity => ClientArguments) {
    val (appId, args) = runApp(f)
    monitorApplication(appId, args.sparkConf)
  }

  def runApp(f: YarnResourceCapacity => ClientArguments) : (ApplicationId, ClientArguments) = {
    val newApp = createYarnApplication
    val appId = newApp.getApplicationId
    val args = f(getClusterResourceCapacity(newApp))
    logClusterResourceDetails(args)

    verifyClusterResources(args,newApp)
    val appContext = createApplicationSubmissionContext(args,appId)
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(args,appStagingDir)
    val env = setupLaunchEnv(args,localResources, appStagingDir)
    val amContainer = createContainerLaunchContext(args,newApp, localResources, env)

    val capability = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    // Memory for the ApplicationMaster.
    capability.setMemory(args.amMemory + args.memoryOverhead)
    amContainer.setResource(capability)

    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setUser(UserGroupInformation.getCurrentUser().getShortUserName())

    submitApp(appContext)
    (appId, args)
  }

  override
  protected def getAppProgress(report: ApplicationReport): YarnAppProgress = {

    val appUsageReport = report.getApplicationResourceUsageReport
    YarnAppProgress(report.getApplicationId.getId,
      getResourceUsage(appUsageReport))
  }


  def logClusterResourceDetails(args: ClientArguments) {
    val clusterMetrics: YarnClusterMetrics = super.getYarnClusterMetrics
    logInfo("Got Cluster metric info from ASM, numNodeManagers = " +
      clusterMetrics.getNumNodeManagers)

    val queueInfo: QueueInfo = super.getQueueInfo(args.amQueue)
    logInfo( """Queue info ... queueName = %s, queueCurrentCapacity = %s, queueMaxCapacity = %s,
      queueApplicationCount = %s, queueChildQueueCount = %s""".format(
        queueInfo.getQueueName,
        queueInfo.getCurrentCapacity,
        queueInfo.getMaximumCapacity,
        queueInfo.getApplications.size,
        queueInfo.getChildQueues.size))
  }


  def createApplicationSubmissionContext(args: ClientArguments,appId: ApplicationId): ApplicationSubmissionContext = {
    logInfo("Setting up application submission context for ASM")
    val appContext = Records.newRecord(classOf[ApplicationSubmissionContext])
    appContext.setApplicationId(appId)
    appContext.setApplicationName(args.appName)
    appContext
  }

  def calculateAMMemory(args: ClientArguments,newApp: GetNewApplicationResponse): Int = {
    val minResMemory = newApp.getMinimumResourceCapability().getMemory()
    val amMemory = ((args.amMemory / minResMemory) * minResMemory) +
      ((if ((args.amMemory % minResMemory) == 0) 0 else minResMemory) - args.memoryOverhead)
    amMemory
  }

  def setupSecurityToken(amContainer: ContainerLaunchContext) = {
    // Setup security tokens.
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setContainerTokens(ByteBuffer.wrap(dob.getData()))
  }

  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Submit the application to the applications manager.
    logInfo("Submitting application to ASM")
    super.submitApplication(appContext)
  }

  def monitorApplication(appId: ApplicationId, sparkConf: SparkConf): Boolean = {
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)

    val initialReport = getApplicationReport(appId)
    notifyAppStart(initialReport)
    while (true) {
      Thread.sleep(interval)
      val report = getApplicationReport(appId)

      val state = report.getYarnApplicationState()

      logProgress(appId, report)

      state match {
        case YarnApplicationState.RUNNING =>
          notifyAppProgress(report)
        case YarnApplicationState.FINISHED =>
          notifyAppFinished(report)
        case YarnApplicationState.FAILED =>
          notifyAppFailed(report)
        case YarnApplicationState.KILLED =>
          notifyAppKilled(report)
        case _ =>
          notifyAppProgress(report)
      }


      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        return true
      }
    }
    true
  }

  private def logProgress(appId: ApplicationId, report: ApplicationReport) {

    logInfo("Application report from ASM: \n" +
      "\t application identifier: " + appId.toString() + "\n" +
      "\t appId: " + appId.getId() + "\n" +
      "\t clientToken: " + report.getClientToken() + "\n" +
      "\t appDiagnostics: " + report.getDiagnostics() + "\n" +
      "\t appMasterHost: " + report.getHost() + "\n" +
      "\t appQueue: " + report.getQueue() + "\n" +
      "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
      "\t appStartTime: " + report.getStartTime() + "\n" +
      "\t yarnAppState: " + report.getYarnApplicationState() + "\n" +
      "\t distributedFinalState: " + report.getFinalApplicationStatus() + "\n" +
      "\t appTrackingUrl: " + report.getTrackingUrl() + "\n" +
      "\t appUser: " + report.getUser()
    )
  }

}

object Client {

  def main(argStrings: Array[String]) {
    if (!sys.props.contains("SPARK_SUBMIT")) {
      println("WARNING: This client is deprecated and will be removed in a " +
        "future version of Spark. Use ./bin/spark-submit with \"--master yarn\"")
    }

    // Set an env variable indicating we are running in YARN mode.
    // Note that anything with SPARK prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")

    val sparkConf = new SparkConf

    try {
      //default implementation ignore yarn capacity
      def  toArgs (capacity: YarnResourceCapacity) = new ClientArguments(argStrings, sparkConf)
      new Client().run(toArgs)
    } catch {
      case e: Exception => {
        Console.err.println(e.getMessage)
        System.exit(1)
      }
    }

    System.exit(0)
  }
}
