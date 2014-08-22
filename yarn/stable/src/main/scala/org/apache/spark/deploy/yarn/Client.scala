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

import org.apache.spark.deploy.yarn._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{YarnClientApplication, YarnClient}
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.Records
import org.apache.spark.deploy.ClientArguments

import org.apache.spark.{Logging, SparkConf}
import scala.collection.mutable.ListBuffer
import java.util.Date

/**
 * Version of [[org.apache.spark.deploy.yarn.ClientBase]] tailored to YARN's stable API.
 */
class Client( hadoopConf: Configuration, toArgs: YarnResourceCapacity => ClientArguments)
  extends ClientBase with Logging {

  def this(to: YarnResourceCapacity => ClientArguments) = this(new Configuration(), to)
  val yarnClient = YarnClient.createYarnClient
  val conf = hadoopConf
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)


  def createYarnApplication : YarnClientApplication = {
    // Initialize and start the client service.
    yarnClient.init(yarnConf)
    yarnClient.start()

    // Prepare to submit a request to the ResourceManager (specifically its ApplicationsManager (ASM)
    // interface).
    // Get a new client application.
    yarnClient.createApplication()
  }

  def run() {
    val (appId, args) = runApp()
    monitorApplication(appId, args.sparkConf)
  }

  def runApp():(ApplicationId, ClientArguments) = {
    val newApp = createYarnApplication
    val args = toArgs(getClusterResourceCapacity(newApp))

    // Log details about this YARN cluster (e.g, the number of slave machines/NodeManagers).
    logClusterResourceDetails(args)
    val appId = prepareAndSubmitApp(args, newApp)
    (appId, args)
  }

  private  def prepareAndSubmitApp(args: ClientArguments, newApp: YarnClientApplication): ApplicationId = {

    val newAppResponse = newApp.getNewApplicationResponse()
    val appId = newAppResponse.getApplicationId()

    verifyClusterResources(args,newAppResponse)

    // Set up resource and environment variables.
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(args,appStagingDir)
    val launchEnv = setupLaunchEnv(args, localResources, appStagingDir)
    val amContainer = createContainerLaunchContext(args, newAppResponse, localResources, launchEnv)

    // Set up an application submission context.
    val appContext = newApp.getApplicationSubmissionContext()
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setApplicationType("SPARK")

    // Memory for the ApplicationMaster.
    val memoryResource = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    memoryResource.setMemory(args.amMemory + args.memoryOverhead)
    appContext.setResource(memoryResource)

    // Finally, submit and monitor the application.
    submitApp(appContext)
    appId
  }

  @Override
  def getClusterResourceCapacity(newApp: YarnClientApplication): YarnResourceCapacity = {
    val newAppResponse = newApp.getNewApplicationResponse()
    val maxCapacity = newAppResponse.getMaximumResourceCapability
    new YarnResourceCapacity(new YarnAppResource(maxCapacity.getMemory, maxCapacity.getVirtualCores), YarnAllocationHandler.MEMORY_OVERHEAD)
  }

  private def logClusterResourceDetails(args: ClientArguments) {
    val clusterMetrics: YarnClusterMetrics = yarnClient.getYarnClusterMetrics
    logInfo("Got cluster metric info from ResourceManager, number of NodeManagers: " +
      clusterMetrics.getNumNodeManagers)
  }

  def calculateAMMemory(args: ClientArguments, newApp: GetNewApplicationResponse): Int = {
    // TODO: Need a replacement for the following code to fix -Xmx?
    // val minResMemory: Int = newApp.getMinimumResourceCapability().getMemory()
    // var amMemory = ((args.amMemory / minResMemory) * minResMemory) +
    //  ((if ((args.amMemory % minResMemory) == 0) 0 else minResMemory) -
    //    memoryOverhead )
    args.amMemory
  }

  def setupSecurityToken(amContainer: ContainerLaunchContext) = {
    // Setup security tokens.
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData()))
  }

  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Submit the application to the applications manager.
    logInfo("Submitting application to ResourceManager")
    yarnClient.submitApplication(appContext)
  }

  def getApplicationReport(appId: ApplicationId) =
    yarnClient.getApplicationReport(appId)

  def stop = yarnClient.stop



  protected def getAppProgress(report: ApplicationReport): YarnAppProgress = {

    val appUsageReport = report.getApplicationResourceUsageReport
    YarnAppProgress(report.getApplicationId.getId,
      getResourceUsage(appUsageReport),
      report.getProgress)
  }



  def monitorApplication(appId: ApplicationId, sparkConf: SparkConf): Boolean = {
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)

    val initReport = yarnClient.getApplicationReport(appId)
    notifyAppStart(initReport)

    while (true) {
      Thread.sleep(interval)
      val report = yarnClient.getApplicationReport(appId)

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
    logInfo("Application report from ResourceManager: \n" +
      "\t application identifier: " + appId.toString() + "\n" +
      "\t appId: " + appId.getId() + "\n" +
      "\t clientToAMToken: " + report.getClientToAMToken() + "\n" +
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
    // Note: anything env variable with SPARK_ prefix gets propagated to all (remote) processes -
    // see Client#setupLaunchEnv().
    val sparkConf = new SparkConf()
    sparkConf.set("SPARK_YARN_MODE", "true")
    try {
      //implementation ignore the yarn resource capacity
      //this matches the current behavior
      def toArgs(capacity: YarnResourceCapacity) = new ClientArguments(argStrings, sparkConf)
      val client = new Client(toArgs)
      client.run()
    } catch {
      case e: Exception => {
        Console.err.println(e.getMessage)
        System.exit(1)
      }
    }

    System.exit(0)
  }

}
