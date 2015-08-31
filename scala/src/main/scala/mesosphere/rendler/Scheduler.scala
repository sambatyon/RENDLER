package mesosphere.rendler

import org.apache.mesos
import mesos._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import java.io.File
import java.nio.charset.Charset

import scala.concurrent.duration.Duration
import scala.util.Try

class Scheduler(val rendlerHome: File, seedURL: String)
    extends mesos.Scheduler
    with ResultProtocol
    with TaskUtils
    with GraphVizUtils {

  protected[this] val crawlQueue = mutable.Queue[String](seedURL)
  protected[this] val renderQueue = mutable.Queue[String](seedURL)

  protected[this] val processedURLs = mutable.Set[String]()
  protected[this] val crawlResults = mutable.Buffer[Edge]()
  protected[this] val renderResults = mutable.Map[String, String]()

  private[this] var tasksCreated = 0
  private[this] var tasksRunning = 0
  private[this] var shuttingDown: Boolean = false

  def waitForRunningTasks(): Unit = {
    while (tasksRunning > 0) {
      println(s"Shutting down but still have $tasksRunning tasks running.")
      Thread.sleep(3000)
    }
  }

  def shutdown[T](maxWait: Duration)(callback: => T): Unit = {
    println("Scheduler shutting down...")
    shuttingDown = true

    val f = Future { waitForRunningTasks() }
    Try { Await.ready(f, maxWait) }

    writeDot(crawlResults, renderResults.toMap, new File(rendlerHome, "result.dot"))
    callback
  }

  def printQueueStatistics(): Unit = println(s"""
    |Queue Statistics:
    |  Crawl queue length:  [${crawlQueue.size}]
    |  Render queue length: [${renderQueue.size}]
    |  Running tasks:       [$tasksRunning]
  """.stripMargin)

  def disconnected(driver: SchedulerDriver): Unit =
    println("Disconnected from the Mesos master...")

  def error(driver: SchedulerDriver, msg: String): Unit =
    println(s"ERROR: [$msg]")

  def executorLost(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    status: Int): Unit =
    println(s"EXECUTOR LOST: [${executorId.getValue}]")

  def frameworkMessage(
    driver: SchedulerDriver,
    executorId: Protos.ExecutorID,
    slaveId: Protos.SlaveID,
    data: Array[Byte]): Unit = {
    import play.api.libs.json._

    println(s"Received a framework message from [${executorId.getValue}]")

    val jsonString = new String(data, Charset.forName("UTF-8"))

    executorId.getValue match {
      case id if id == crawlExecutor.getExecutorId.getValue =>
        val result = Json.parse(jsonString).as[CrawlResult]
        for (link <- result.links) {
          val edge = Edge(result.url, link)
          println(s"Appending [$edge] to crawl results")
          crawlResults += edge
          if (!processedURLs.contains(link)) {
            println(s"Enqueueing [$link]")
            crawlQueue += link
            renderQueue += link
            processedURLs += link
          }
        }

      case id if id == renderExecutor.getExecutorId.getValue =>
        val result = Json.parse(jsonString).as[RenderResult]
        val mapping = result.url -> result.imageUrl
        println(s"Appending [$mapping] to render results")
        renderResults += mapping

      case _ => ()
    }
  }

  def offerRescinded(
    driver: SchedulerDriver,
    offerId: Protos.OfferID): Unit =
    println(s"Offer [${offerId.getValue}] has been rescinded")

  def registered(
    driver: SchedulerDriver,
    frameworkId: Protos.FrameworkID,
    masterInfo: Protos.MasterInfo): Unit = {
    val host = masterInfo.getHostname
    val port = masterInfo.getPort
    println(s"Registered with Mesos master [$host:$port]")
  }

  def reregistered(
    driver: SchedulerDriver,
    masterInfo: Protos.MasterInfo): Unit = ???

  def resourceOffers(
    driver: SchedulerDriver,
    offers: java.util.List[Protos.Offer]): Unit = {

    printQueueStatistics()

    for (offer <- offers.asScala) {
      println(s"Got resource offer [$offer]")

      if (shuttingDown) {
        println(s"Shutting down: declining offer on [${offer.getHostname}]")
        driver.declineOffer(offer.getId)
      }
      else {
        val maxTasks = maxTasksForOffer(offer)

        val tasks = mutable.Buffer[Protos.TaskInfo]()

        for (_ <- 0 until maxTasks / 2) {
          if (crawlQueue.nonEmpty) {
            val url = crawlQueue.dequeue
            tasks += makeCrawlTask(s"$tasksCreated", url, offer)
            tasksCreated = tasksCreated + 1
          }
          if (renderQueue.nonEmpty) {
            val url = renderQueue.dequeue
            tasks += makeRenderTask(s"$tasksCreated", url, offer)
            tasksCreated = tasksCreated + 1
          }
        }

        if (tasks.nonEmpty)
          driver.launchTasks(Seq(offer.getId).asJava, tasks.asJava)
        else
          driver.declineOffer(offer.getId)
      }
    }
  }

  def slaveLost(
    driver: SchedulerDriver,
    slaveId: Protos.SlaveID): Unit =
    println("SLAVE LOST: [${slaveId.getValue}]")

  def statusUpdate(
    driver: SchedulerDriver,
    taskStatus: Protos.TaskStatus): Unit = {
    val taskId = taskStatus.getTaskId.getValue
    val state = taskStatus.getState
    println(s"Task [$taskId] is in state [$state]")
    if (state == Protos.TaskState.TASK_RUNNING)
      tasksRunning = tasksRunning + 1
    else if (isTerminal(state))
      tasksRunning = math.max(0, tasksRunning - 1)
  }

}