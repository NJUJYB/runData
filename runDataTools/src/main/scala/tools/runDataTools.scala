package tools

import java.io.{File, FileReader, BufferedReader}

import akka.actor.{ActorLogging, Actor}
import akka.pattern.ask
import akka.util.Timeout
import org.apache.spark.runhdfs.runMessage.proActiveDataMessage.{RequestRunDataNonExist, RequestRunDataExist}

import scala.concurrent._
import scala.concurrent.duration._

/**
  * Created by jyb on 10/6/18.
  */

class runDataTools extends Actor with ActorLogging{
  var remoteActor = context.actorSelection("akka.tcp://sparkMaster@master:7077/user/Master")
  implicit val timeout = Timeout(1 seconds)

  override def receive: Receive = {
    case message: String =>{
      val infos: Array[String] = message.split("#")
      var future: Future[Any] = null
      val localHost = findLocalHost()
      if(infos.length == 2){
        future = remoteActor ? RequestRunDataExist(infos(0), localHost, infos(1))
      }else if(infos.length == 3){
        future = remoteActor ? RequestRunDataNonExist(infos(0), infos(1), localHost, infos(2))
      }
      val result = Await.result(future, timeout.duration)
    }
  }

  def findLocalHost(): String = {
    val br: BufferedReader = new BufferedReader(new FileReader(new File("/home/jyb/.ssh/id_rsa.pub")))
    val line: String  = br.readLine()
    br.close()
    if(line != null) {
      val infos: Array[String] = line.split("@")
      return infos(infos.length - 1)
    }
    return null
  }
}

