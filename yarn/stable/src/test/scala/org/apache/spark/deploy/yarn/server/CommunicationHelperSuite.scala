package org.apache.spark.deploy.yarn.server

import org.apache.spark.{SparkConf, Logging}

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class CommunicationHelperSuite extends FunSuite with BeforeAndAfterAll with Matchers with Logging {

  test("create messenger") {

    val sparkConf = new SparkConf()
    val helper = CommunicationHelper
    val appCtx = new ApplicationContext(sparkConf)
    val messenger = helper.createRelayMessenger(appCtx)

   // assert(message == )

    //todo : need finish tests


  }
}
