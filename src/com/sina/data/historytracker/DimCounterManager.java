package com.sina.data.historytracker;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DimCounterManager {
  public static final Logger LOG = Logger.getLogger(DimCounterManager.class.getName());
  private Configuration conf;
  private Connection dbConn;
  private HashMap<String, Short> mapping = new HashMap<String, Short>();
  
  public DimCounterManager(Configuration c, Connection conn) throws SQLException {
    this.conf = c;
    this.dbConn = conn;
    this.update();
  }
  
  private void update() throws SQLException {
    Statement stmt = dbConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from DimCounter");
    while (rs.next()) {
      short id = rs.getShort("CounterID");
      String desc = rs.getString("CounterDesc");
      mapping.put(desc, new Short(id));
      if (LOG.isDebugEnabled()){
        LOG.debug("Get " + id + " => " + desc);
      }
    } 
    rs.close();
    stmt.close();
  }
  
  public short getID(String counterKey) throws SQLException {
    Short id = mapping.get(counterKey);
    if (id == null || id.intValue() == -1) {
      id = realInsert(counterKey);
      mapping.put(counterKey, id);
    }
    return id;
  }
  
  private short realInsert(String counterKey) throws SQLException {
    Statement stmt = dbConn.createStatement();
    stmt.execute("insert into DimCounter(CounterDesc) values ('" + counterKey + "');");
    dbConn.commit();
    ResultSet rs = stmt.executeQuery("select CounterID from DimCounter where CounterDesc = '" 
        + counterKey + "'");
    short id = -1;
    while(rs.next()) {
      id = rs.getShort(1);
      break;
    }
    rs.close();
    stmt.close();
    LOG.debug("Added a new CounterKey " + counterKey + " and assigned it ID " + id);
    return id;
  }
  
}
