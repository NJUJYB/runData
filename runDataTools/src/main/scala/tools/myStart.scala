package tools

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
  * Created by jyb on 10/7/18.
  */
class myStart {
  var localHost: String = null

  def run(args: Array[String]): Unit ={
    val system = ActorSystem("runData", ConfigFactory.parseString(
      """
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      """.stripMargin))
    val localActor = system.actorOf(Props[runDataTools])
    var str: String = ""
    for(i <- 0 until args.length){
      str += args(i)
      if(i != (args.length - 1)) str += "#"
    }
    localActor ! str
    Thread.sleep(500)
    system.shutdown()
  }
}
