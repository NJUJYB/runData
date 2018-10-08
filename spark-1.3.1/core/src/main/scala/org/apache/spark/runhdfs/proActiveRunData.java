package org.apache.spark.runhdfs;

import java.io.File;

/**
 * Created by jyb on 10/6/18.
 */
public class proActiveRunData {
  public static String targetDirPath = "/home/jyb/Desktop/hadoop/hadoop-2.2.0/logs/blockCache/";

  public static void proActiveDataExsits(String hdfsFilePath){
	checkDirExist();
	String[] infos = hdfsFilePath.split("/");
	String fileName = infos[infos.length - 1];
	String localFilePath = targetDirPath + fileName;
	File localFile = new File(localFilePath);
	if(localFile.exists()) localFile.delete();
	String cmd1 = validCachedFile.hdfsSH + " -get " + hdfsFilePath + " " + localFilePath;
	validCachedFile.execCMD(cmd1);
	validCachedFile.recordLogs("CMD Info: Download File End");
	validCachedFile.validFile(localFilePath, hdfsFilePath, fileName);
  }

  public static void proActiveDataDirectly(String remoteLocalFilePath, String hdfsDir, String remoteHost){
	checkDirExist();
	String[] infos = remoteLocalFilePath.split("/");
	String fileName = infos[infos.length - 1];
	String localFilePath = targetDirPath + fileName;
	File localFile = new File(localFilePath);
	if(localFile.exists()) localFile.delete();
	String cmd1 = "scp jyb@" + remoteHost + ":" + remoteLocalFilePath + " " + localFilePath;
	validCachedFile.execCMD(cmd1);
	validCachedFile.recordLogs("CMD Info: proActive File Download End");
	String cmd2 = validCachedFile.hdfsSH + " -mkdir -p " + hdfsDir;
	validCachedFile.execCMD(cmd2);
	validCachedFile.recordLogs("CMD Info: Check HDFS Path");
	String cmd3 = validCachedFile.hdfsSH + " -put " + localFilePath + " " + hdfsDir;
	validCachedFile.execCMD(cmd3);
	validCachedFile.recordLogs("CMD Info: proActive File Valid End");
  }

  public static void checkDirExist(){
	File tDir = new File(targetDirPath);
	if(!tDir.exists() && !tDir.isDirectory()) tDir.mkdirs();
  }
}