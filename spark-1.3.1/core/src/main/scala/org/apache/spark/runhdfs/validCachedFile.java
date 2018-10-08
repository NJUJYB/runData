package org.apache.spark.runhdfs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by jyb on 10/6/18.
 */
public class validCachedFile {
  public static ArrayList<String> eventsLogs;
  public static ArrayList<Long> timeLogs;
  public static String hdfsSH = "/home/jyb/Desktop/hadoop/hadoop-2.2.0/bin/hdfs dfs";

  public static void validFile(String localFilePath, String hdfsFilePath, String fileName){
    String hdfsDir = hdfsFilePath.split(fileName)[0];
    String cmd1 = hdfsSH + " -rm " + hdfsFilePath;
    String cmd2= " && " + hdfsSH + " -put " + localFilePath + " " + hdfsDir;
    String cmd = cmd1 + cmd2;
    execCMD(cmd);
    recordLogs("CMD Info: End Copy From Task");
  }

  public static void execCMD(String cmd){
    logsInitial();
    recordLog(cmd);
    try{
      String[] command = {"/bin/sh", "-c", cmd};
      Process process = Runtime.getRuntime().exec(command);
      BufferedReader stdoutReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      BufferedReader stderrReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      recordLog("Before Start CMD");
      String line;
      while((line = stdoutReader.readLine()) != null){
        recordLog("Out: " + line);
      }
      while((line = stderrReader.readLine()) != null){
        recordLog("Error: " + line);
      }
      int exitVal = process.waitFor();
      recordLog("" + exitVal);
      recordLog("End CMD");
    } catch (Exception e) {
      recordLog("Exception: " + e.toString());
      e.printStackTrace();
    }
  }

  public static void recordLog(String event){
    long time = System.nanoTime();
    eventsLogs.add(event);
    timeLogs.add(time);
  }

  public static void logsInitial(){
    eventsLogs = new ArrayList<String>();
    timeLogs = new ArrayList<Long>();
  }

  public static void recordLogs(String preString){
    runDataLogs.writeLogs(preString, eventsLogs, timeLogs);
  }
}