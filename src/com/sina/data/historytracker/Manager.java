package com.sina.data.historytracker;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.Timer;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Manager {

  static{
    Configuration.addDefaultResource(Constants.CONFFILE);
    PropertyConfigurator.configure("conf/log4j.properties");
  }
  
  public static final Logger LOG = Logger.getLogger(Manager.class.getName());
  
  public static void main(String[] args) {
    if (args.length != 1) {
      printUsage();
      return;
    }
    if (args[0].equalsIgnoreCase("-startMonitor")) {
      try {
        startMonitor();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    else if (args[0].equalsIgnoreCase("-catchUp"))
      try {
        catchUp();
      } catch (Exception e) {
        e.printStackTrace();
      }
    else if (args[0].equalsIgnoreCase("-updateAccess")) {
      try {
        updateAccess(args);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
    else 
      printUsage();
  }
  
  private static void updateAccess(String[] args) throws SQLException {
    LOG.info("############Starting updateAccess############");
    Configuration conf = new Configuration();
    ImageAccessUpdater iau = null;
    try {
      iau = new ImageAccessUpdater(conf);
      iau.run();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if (iau != null)
        iau.close();
    }
    
  }

  public static void startMonitor() throws Exception {
    LOG.info("############Haha, I am alive############");
    Configuration conf = new Configuration();
    String baseDir = conf.get("history.rootdir");
    if (baseDir == null || baseDir.equals("")) {
      System.err.println("Please set history.rootdir in config file");
      return;
    }
    if (!baseDir.endsWith("/"))
      baseDir = baseDir + "/";
    
    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    Timer workerTimer = new Timer();
    Worker worker = new Worker(conf, queue);
    workerTimer.scheduleAtFixedRate(worker, 0, 1000);
    Timer monitorTimer = new Timer();
    
    Calendar lastCal = Calendar.getInstance();
    DirectoryMonitor currMonitor = new DirectoryMonitor(conf, 
                                          getPathString(baseDir, lastCal), 
                                          queue);
    monitorTimer.scheduleAtFixedRate(currMonitor, 0, 60*1000); // TODO
    
    while(true) {
      try {
        Thread.sleep(30*1000);
      } catch (InterruptedException e) {
      }
      Calendar currCal = Calendar.getInstance();
      if (currCal.get(Calendar.DAY_OF_MONTH) != 
          lastCal.get(Calendar.DAY_OF_MONTH)) {
        lastCal = currCal;
        monitorTimer.cancel();
        DirectoryMonitor tempMonitor = new DirectoryMonitor(conf, 
                                            getPathString(baseDir, currCal), 
                                            queue,
                                            false);
        monitorTimer = new Timer();
        monitorTimer.scheduleAtFixedRate(tempMonitor, 0, 60*1000);
        // make sure the last DirectoryMonitor finishes its work
        try {
          Thread.sleep(60*1000);
        } catch (InterruptedException e) {
        }
        currMonitor.run();
        currMonitor = tempMonitor;
      }
    }
  }
  
  private static String getPathString(String baseDir, Calendar cal) {
    StringBuffer sb = new StringBuffer(baseDir);
    sb.append(cal.get(Calendar.YEAR)).append("/");
    int month = cal.get(Calendar.MONTH) + 1;
    if (month >= 10) {
      sb.append(month).append("/");
    } else {
      sb.append(0).append(month).append("/");
    }
    int day = cal.get(Calendar.DAY_OF_MONTH);
    if (day >= 10) {
      sb.append(day).append("/");
    } else {
      sb.append(0).append(day).append("/");
    }
    return sb.toString();
  }
  
  public static void catchUp() throws Exception {
    LOG.info("############Starting catchup############");
    Configuration conf = new Configuration();
    String baseDir = conf.get("history.catchup.dir");
    if (baseDir == null || baseDir.equals("")) {
      System.err.println("Please set history.catchup.dir in config file");
      return;
    }
    if (!baseDir.endsWith("/"))
      baseDir = baseDir + "/";
    
    LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<String>();
    Timer workerTimer = new Timer();
    Worker worker = new Worker(conf, queue);
    workerTimer.scheduleAtFixedRate(worker, 0, 100);
    
    //use false to fetch all history files in history.catchup.dir
    DirectoryMonitor currMonitor = new DirectoryMonitor(conf, 
                                          baseDir,  
                                          queue, 
                                          false);
    
    while(true) {
      Thread.sleep(65*1000);
      currMonitor.run();
      if (queue.isEmpty()) {
        LOG.info("Catch up finishes. Exiting ...");
        workerTimer.cancel();
        return;
      }
    }
  
  }

  public static void printUsage() {
    System.out.println("Usage: -startMonitor | -catchUp | -updateAccess");
  }
}
