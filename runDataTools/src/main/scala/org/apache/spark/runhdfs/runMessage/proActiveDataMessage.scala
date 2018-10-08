package org.apache.spark.runhdfs.runMessage

/**
  * Created by jyb on 10/7/18.
  */
trait proActiveDataMessage extends Serializable

object proActiveDataMessage {
  case class RequestRunDataExist(hdfsFilePath: String, originHost: String, targetHost: String)
    extends proActiveDataMessage

  case class RequestRunDataNonExist(localFilePath: String, hdfsDir: String,
    originHost: String, targetHost: String) extends proActiveDataMessage
}
