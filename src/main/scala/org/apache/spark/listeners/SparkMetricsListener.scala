package org.apache.spark.listeners

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.logging.Logger

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerTaskEnd}

/**
 * Spark listener class to handle the Spark events
 */
class SparkMetricsListener extends SparkListener {
  private val log = Logger.getLogger(getClass.getName)

  private val jobsCompleted   = new AtomicInteger(0)
  private val stagesCompleted = new AtomicInteger(0)
  private val tasksCompleted = new AtomicInteger(0)
  private val executorRuntime = new AtomicLong(0L)
  private val recordsRead     = new AtomicLong(0L)
  private val recordsWritten  = new AtomicLong(0L)

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    log.info("***************** Aggregate metrics *****************************")
    log.info(s"* Jobs = ${jobsCompleted.get()}, Stages = ${stagesCompleted.get()}, Tasks = ${tasksCompleted}")
    log.info(s"* Executor runtime = ${executorRuntime.get()}ms, Records Read = ${recordsRead.get()}, Records written = ${recordsWritten.get()}")
    log.info("*****************************************************************")
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = jobsCompleted.incrementAndGet()

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = stagesCompleted.incrementAndGet()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    tasksCompleted.incrementAndGet()
    executorRuntime.addAndGet(taskEnd.taskMetrics.executorRunTime)
    recordsRead.addAndGet(taskEnd.taskMetrics.inputMetrics.recordsRead)
    recordsWritten.addAndGet(taskEnd.taskMetrics.outputMetrics.recordsWritten)
  }

}
