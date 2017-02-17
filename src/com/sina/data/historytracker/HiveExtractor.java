package com.sina.data.historytracker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.Logger;

public class HiveExtractor {
  
  public static final Logger LOG = Logger.getLogger(Worker.class.getName());
  public static final String catchupConfKey = "history.hive.catchup.ts";
  
  private HiveConf conf = null;
  private Driver hDriver = null;
  private Connection conn = null;
  private Statement stmt = null;
  private Timestamp tsTo = null;
  private Timestamp tsFrom = null;
  
  public HiveExtractor() throws SQLException {
    HiveConf conf = new HiveConf();
    conf.addResource("hive-default.xml");
    init();
  }

  public HiveExtractor(HiveConf hc) throws SQLException {
    this.conf  = hc;
    init();
  }
  
  private void init() throws SQLException {
    CliSessionState css = new CliSessionState(new HiveConf(SessionState.class));
    css.out = System.out;
    css.err = System.err;
    SessionState.start(css);
    hDriver  = new Driver(conf);
    
    String url = conf.get("history.tracker.db.connection");
    if (url == null || url.equals("")) {
      LOG.error("Need a connection specified in conf file!");
      return;
    }
    String dbUser = conf.get("history.tracker.db.user");
    String dbPasswd = conf.get("history.tracker.db.password");
    conn = DriverManager.getConnection(url, dbUser, dbPasswd);
    stmt  = conn.createStatement();
    long confedTS = conf.getLong(catchupConfKey, System.currentTimeMillis() / 1000) * 1000;
    tsTo = new Timestamp(confedTS / (24*3600*1000) * (24*3600*1000) - 8*3600*1000);
    tsFrom = new Timestamp(confedTS / (24*3600*1000) * (24*3600*1000) - 32*3600*1000);
  }
  
  public void run() throws SQLException {
    doWork();
  }
  
  private void doWork() throws SQLException {
    stmt.executeQuery("");
  }
  
  private void parseHQL(String cmd) {
    
  }
  
  public void close() {
    if (hDriver != null)
      hDriver.close();
    
  }
}
