package com.sina.data.historytracker;

import java.io.File;
import java.io.FileFilter;
import java.util.HashSet;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class DirectoryMonitor extends TimerTask {
  
  static{
    Configuration.addDefaultResource(Constants.CONFFILE);
    PropertyConfigurator.configure("conf/log4j.properties");
  }
  
  public static final Logger LOG = Logger.getLogger(DirectoryMonitor.class.getName());
  Configuration conf = null;
  String root = null;
  LinkedBlockingQueue<String> queue;
  Set<String> checkedFiles = new HashSet<String>();
  long lastCheckTime = 0;
  FileFilter ff = new FileFilter() {
    @Override
    public boolean accept(File pathname) {
      String name = pathname.getName();
      if (name.endsWith(".xml") || name.startsWith("."))
        return false;
      try {
        String ts = name.split("_")[3];
        Long.parseLong(ts);
        return true;
      } catch (Exception e) {
        return false;
      }
    }
  };

  /**
   * @param c
   * @param rootDir the root directory to check
   * @param workQ
   *          Emits ONLY the result log file. Users should guess the conf file
   *          using rules.
   */
  public DirectoryMonitor(Configuration c, String rootDir,
      LinkedBlockingQueue<String> workQ) {
    this(c, rootDir, workQ, true);
  }
  
  /**
   * @param c
   * @param rootDir the root directory to check
   * @param workQ
   *          Emits ONLY the result log file. Users should guess the conf file
   *          using rules.
   * @param needToPrefetch If true, rule out existing history files; false other wise; 
   */
  public DirectoryMonitor(Configuration c, String rootDir,
      LinkedBlockingQueue<String> workQ, boolean needToPrefetch) {
    conf = c;
    root = rootDir;
    queue = workQ;
    if (needToPrefetch) {
      LinkedBlockingQueue<String> tempQ = new LinkedBlockingQueue<String>();
      findNewFiles(this.root, tempQ);
      lastCheckTime = System.currentTimeMillis();
      tempQ.clear();
    }
    LOG.info("Starting DirectoryMonitor on " + rootDir);
  }

  /**
   * Check new files at most once a minute.
   */
  public void run() {
    if (findNewFiles(this.root, queue)) {
      lastCheckTime = System.currentTimeMillis();
    }
  }

  private boolean findNewFiles(String thisRoot, LinkedBlockingQueue<String> resultQ) {
    if (lastCheckTime + 60 * 1000 > System.currentTimeMillis())
      return false;
    else {
      File folder = new File(thisRoot);
      if (!folder.isDirectory())
        return false;
      File[] children = folder.listFiles();
      for (File f : children) {
        String name = f.getAbsolutePath();
        if (f.isDirectory())
          findNewFiles(name, resultQ);
        else {
          if (ff.accept(f) && !checkedFiles.contains(name)) {
            resultQ.add(name);
            checkedFiles.add(name);
            if (LOG.isDebugEnabled())
              LOG.debug("Found a log file: " + name);
          }
        }
      }
      
      return true;
    }
  }

  public void close() {

  }
  
  public static void main(String args[]) {
    final LinkedBlockingQueue<String> workQ = new LinkedBlockingQueue<String>();
    Configuration c = new Configuration();
    final DirectoryMonitor dm = new DirectoryMonitor(c, "/host/workspace/jobhistorytracker/testdir1", workQ);
    
    Thread t = new Thread(){
      public void run(){
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          dm.run();
          LOG.info("dm.checkedFiles " + dm.checkedFiles);
          LOG.info("workQ " + workQ);
        }
      }
    };
    t.start();
    LOG.info(dm.checkedFiles);
    LOG.info("dm.checkedFiles " + dm.checkedFiles);
    LOG.info("workQ " + workQ);
    try {
      Thread.sleep(1000*60);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    t.interrupt();
  }
}
