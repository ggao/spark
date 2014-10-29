package org.apache.spark.deploy.yarn.server

import java.io.{PrintWriter, StringWriter}
import java.util.concurrent.TimeUnit

import org.apache.spark.{ResourceUtils, SparkConf}


trait YarnSparkApp {
  import ApplicationContext._

  def sparkMain(appCtx: ApplicationContext)

  def run(conf: SparkConf) {

    var logger = new ChannelMessageLogger("spark app", None)

    logTime { // log time

      var failed = false
      var appContext: Option[ApplicationContext] = None

      try {
        val appCtx = new ApplicationContext(conf)
        appContext = Some(appCtx)
        logger = appCtx.logger
        logger.logInfo(s"starting ${appCtx.appName}")

        sparkMain(appCtx)

      } catch {
        case e: Throwable =>
          failed = true
          val t = wrapThrowable(appContext, e)
          printStackTrace(t)
          throw t
      } finally {
        if (!failed) {
          logger.sendUpdateMessage(SPARK_APP_COMPLETED, true)
          logger.logInfo(s"spark app finished.")
        }
        waitToFinish(failed)
        appContext.foreach(_.stop())
        appContext.foreach(_.restoreConsoleOut())
      }

      def waitToFinish(failed: Boolean) {
        val debugEnabled = conf.get(SPARK_YARN_APP_DEBUG_ENABLED, "false").toBoolean
        if (debugEnabled || failed) {
          val sleepTime = conf.get(SPARK_YARN_APP_SLEEP_TIME_BEFORE_COMPLETE, "0").toInt
          val message: String = s" sleeping for $sleepTime sec before stop"
          logger.logInfo(message)
          appContext.foreach { aCtx =>
            logger.sendVisualMessage(DisplayMessage(s" Wait -- ", message))
          }
          TimeUnit.SECONDS.sleep(sleepTime)
        }
      }
    }


    def logTime[T] ( f :  => T) : T = {
      val start = System.currentTimeMillis()
      val t = f
      val end = System.currentTimeMillis()
      val msg = "total running time: " + (end - start) / 1000 + " sec"
      logger.logInfo(msg)
      t
    }
  }


  def wrapThrowable(appContext: Option[ApplicationContext], e: Throwable): RuntimeException = {
    val appName = appContext match {
      case Some(ac) => ac.appName
      case None => ""
    }
    val failedMsg = s"Application $appName failed due to " +
      s"${if (e.getMessage == null && e.getCause != null) e.getCause.getMessage else e.getMessage}"
    new RuntimeException(failedMsg, e)
  }

  def printStackTrace(t: Throwable) {
    ResourceUtils.withResource(new StringWriter()) { s =>
      ResourceUtils.withResource(new PrintWriter(s)) { p =>
        t.printStackTrace(p)
        Console.err.println(s.getBuffer.toString)
      }
    }
  }
}
