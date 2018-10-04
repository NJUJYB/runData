package org.apache.spark.runhdfs;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jyb on 10/3/18.
 */
public class runDataLogs {
  private static File file = new File("/home/jyb/Desktop/hadoop/hadoop-2.2.0/logs/runData-logs.txt");
  private static FileWriter writer;

  public static void initial(){
	try{
	  if(!file.exists()) file.createNewFile();
	} catch (IOException e){
	  e.printStackTrace();
	}
  }

  public static void writeLogs(String name, ArrayList<String> logs, ArrayList<Long> times){
	initial();
	try{
	  writer = new FileWriter(file, true);
	  for(int i = 0; i < logs.size(); ++i)
		writer.write(name + ":" + times.get(i) + "," + logs.get(i) + "\r\n");
	  writer.close();
	} catch (IOException e){
	  e.printStackTrace();
	}
  }
}