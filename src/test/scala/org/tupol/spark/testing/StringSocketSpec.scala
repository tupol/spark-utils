package org.tupol.spark.testing

import java.io.PrintStream
import java.net.{ ServerSocket, Socket }

import org.scalatest.{ BeforeAndAfterAll, Suite }

import scala.util.Try


trait StringSocketSpec extends BeforeAndAfterAll {
  this: Suite =>

  val host = "localhost"
  def port = 9999
  def delayMillis = 1000

  private var _server: ServerSocket = _
  private var _socket: Socket = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _server = Try(new ServerSocket(port)).get
  }

  override def afterAll(): Unit = {
    if (_server != null) _server.close
  }

  def send(record: String): Unit = {
    _socket = if (_socket == null) Try(_server.accept()).get else _socket
    val out = new PrintStream(_socket.getOutputStream())
    out.println(record)
    out.flush()
    Thread.sleep(delayMillis)
  }

  def send(records: Seq[String]): Unit = records.foreach(send)

}
