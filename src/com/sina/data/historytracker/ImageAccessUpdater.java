package com.sina.data.historytracker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;


public class ImageAccessUpdater {
  class Node implements Comparable<Node> {
    String dirName = "";
    Timestamp ts = null;
    long counter = 0;
    
    public Node(String dir, Timestamp thisTs) {
      this.dirName = dir;
      this.ts = thisTs;
    }
    
    public void setDir(String dir) {
      this.dirName = dir;
    }
    
    @Override
    public int compareTo(Node o) {
      return this.dirName.compareTo(o.dirName);
    }
    
    @Override
    public int hashCode() {
      return dirName.hashCode();
    }
    
    public void updateAccess(Timestamp time) {
      if (ts == null || ts.compareTo(time) < 0) {
        ts = time;
      } 
      ++counter;
    }
    
    public String toString() {
      if (ts != null) {
        return "('" + dirName + "','" + ts.toString() + "'," + counter + ")";
      } else {
        return "";
      }
    }
  }
  
  public static final Logger LOG = Logger.getLogger(Manager.class.getName());
  public static final String catchupConfKey = "history.access.catchup.ts";
  private Configuration conf;
  private Connection conn = null;
  private List<Node> buf = new ArrayList<Node>();
  private Statement stmt = null;
  private Timestamp tsFrom ;
  private Timestamp tsTo;
  
  public ImageAccessUpdater(Configuration c) throws SQLException {
    this.conf = c;
    String url = conf.get("history.tracker.db.connection");
    if (url == null || url.equals("")) {
      LOG.error("Need a connection specified in conf file!");
      return;
    }
    String dbUser = conf.get("history.tracker.db.user");
    String dbPasswd = conf.get("history.tracker.db.password");
    conn = DriverManager.getConnection(url, dbUser, dbPasswd);
    stmt  = conn.createStatement();
    long confedTS = c.getLong(catchupConfKey, System.currentTimeMillis() / 1000) * 1000;
    tsTo = new Timestamp(confedTS / (24*3600*1000) * (24*3600*1000) - 8*3600*1000);
    tsFrom = new Timestamp(confedTS / (24*3600*1000) * (24*3600*1000) - 32*3600*1000);
    
    init();
  }
  
  private void init() throws SQLException {
    ResultSet rs = stmt.executeQuery("select dirname, accessTime from image where dele = 0");
    while(rs.next()) {
      buf.add(new Node(rs.getString(1), rs.getTimestamp(2)));
    }
    Collections.sort(buf);
  }
  
  public void run() {
    try {
      doWork();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void doWork() throws SQLException {
    Statement innerstmt = conn.createStatement();
    Node tempNode = new Node("", null);
    long counter = 0;
    String mainQueryString = "select ID, EndTime from JobInfo "
        + "where ID > (select ID-100000 from JobInfo order by `ID` desc limit 1) and EndTime > '"
        + tsFrom.toString() + "' and EndTime < '" + tsTo.toString() + "'";
    // if this run is for catch up, do not use the ID heuristics
    if (conf.getLong(catchupConfKey, 0) != 0) {
      mainQueryString = "select ID, EndTime from JobInfo "
          + "where EndTime > '" + tsFrom.toString() + "' and EndTime < '"
          + tsTo.toString() + "'";
      LOG.info("Running in catchup mode");
    }
    LOG.info(mainQueryString);
    ResultSet rs = stmt.executeQuery(mainQueryString);
    while (rs.next()) {
      ++counter;
      HashSet<Integer> uniqDir = new HashSet<Integer>();
      int jobID = rs.getInt(1);
      Timestamp ts = rs.getTimestamp(2);
      ResultSet innerRs = innerstmt.executeQuery("select InputDir from JobInput where JobID=" + jobID);
      while (innerRs.next()) {
        String raw = innerRs.getString(1);
        findRealDirs(tempNode, raw, uniqDir);
      }
      for (Integer e : uniqDir) {
        buf.get(e).updateAccess(ts);
      }
    } // end of while

    LOG.info("Found " + counter + " jobs");

    PreparedStatement imagePst = conn.prepareStatement("update image set accessTime=?, "
            + "counter=counter+? where dirname = ?");
    long tag = 0;
    for (Node n : buf) {
      if (n.counter > 0) {
        imagePst.setTimestamp(1, n.ts);
        imagePst.setLong(2, n.counter);
        imagePst.setString(3, n.dirName);
        imagePst.addBatch();
        if (++tag % 1000 == 0)
          LOG.info("Have updated " + tag + " records");
      }
    }
    imagePst.executeBatch();

    innerstmt.close();
  }

  private void expendReg(Node tempNode, String raw, HashSet<Integer> jobDirs) {
    if (raw.indexOf('*') != -1 && raw.indexOf('?') != -1) {
      return;
    }
    Pattern pattern = Pattern.compile(StringUtils.unEscapeString(raw)
        .replaceAll("\\*", ".*").replace("\\?", ".?"));
    int regBeginPos1 = raw.indexOf('*');
    regBeginPos1 = regBeginPos1 == -1 ? Integer.MAX_VALUE : regBeginPos1;
    int regBeginPos2 = raw.indexOf('?');
    regBeginPos2 = regBeginPos2 == -1 ? Integer.MAX_VALUE : regBeginPos2;
    // find the beginning of regex
    int regBeginPos = regBeginPos1 > regBeginPos2 ? regBeginPos2 : regBeginPos1;
    regBeginPos = regBeginPos > raw.length() ? raw.length() : regBeginPos;
    String prefix = raw.substring(0, regBeginPos);
    tempNode.setDir(prefix);
    int searchCurrPoint = Collections.binarySearch(buf, tempNode);
    if (searchCurrPoint < 0) {
      searchCurrPoint = searchCurrPoint * -1 - 1;
    }
    while (searchCurrPoint < buf.size()
        && buf.get(searchCurrPoint).dirName.startsWith(prefix)) {
      String thisStr = buf.get(searchCurrPoint).dirName;
      if (pattern.matcher(thisStr).matches()) {
        jobDirs.add(searchCurrPoint);
      }
      ++searchCurrPoint;
    }
    return;
  }

  public void findRealDirs(Node tempNode, String path, HashSet<Integer> jobDirs) {
    Path srcPath = new Path(path);
    int depthToStop = 0;
    String components[] = StringUtils.split(path, StringUtils.ESCAPE_CHAR, '/');
    for(int i = 0; i < components.length && isEffectiveComponent(components[i]); ++i) {
      ++depthToStop;
    }
    while(srcPath != null && srcPath.depth() > depthToStop - 2) {
      if (srcPath.getName().indexOf('*') != -1 || 
          srcPath.getName().indexOf('?') != -1) {
        expendReg(tempNode, srcPath.toUri().getPath(), jobDirs);
      }
      tempNode.setDir(srcPath.toUri().getPath());
      int pos = Collections.binarySearch(buf, tempNode);
      if (pos < 0) { // no such file or dir, ignore it
        srcPath = srcPath.getParent();
        continue;
      }
      jobDirs.add(pos);
      srcPath = srcPath.getParent();
    }
  }
  
  private boolean isEffectiveComponent(String str) {
    try {
      Long.parseLong(str);
    } catch (NumberFormatException e) {
      if (str.startsWith("dt="))
        return false;
      return true;
    }
    return false;
  }

  public void close() throws SQLException {
    if (stmt != null && !stmt.isClosed())
      stmt.close();
    if (conn != null && !conn.isClosed())
      conn.close();
  }
}
