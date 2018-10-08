import tools.myStart;

/**
 * Created by jyb on 10/7/18.
 */
public class Start {
  public static void main(String[] args){
    myStart ms = new myStart();
    ms.run(args);
    // runData-Exist:     hdfsFilePath, targetHost
    // runData-NonExist:  localFilePath, hdfsDir, targetHost
  }
}