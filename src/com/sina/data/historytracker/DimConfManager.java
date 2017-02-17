package com.sina.data.historytracker;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class DimConfManager {
  public static final Logger LOG = Logger.getLogger(DimConfManager.class.getName());
  private Configuration conf;
  private Connection dbConn;
  private HashMap<String, Short> mapping = new HashMap<String, Short>();
  
  public DimConfManager(Configuration c, Connection conn) throws SQLException {
    this.conf = c;
    this.dbConn = conn;
    update();
  }
  
  synchronized private void update() throws SQLException {
    Statement stmt = dbConn.createStatement();
    ResultSet rs = stmt.executeQuery("select * from DimConf");
    while (rs.next()) {
      short id = rs.getShort("ConfID");
      String desc = rs.getString("ConfDesc");
      mapping.put(desc, new Short(id));
      if (LOG.isDebugEnabled()){
        LOG.debug("Get " + id + " => " + desc);
      }
    }
    rs.close();
    stmt.close();
  }
  
  public short getID(String confKey) throws SQLException {
    Short id = mapping.get(confKey);
    if (id == null || id.intValue() == -1) {
      id = realInsert(confKey);
      mapping.put(confKey, id);
    }
    return id;
  }
  
  private short realInsert(String confKey) throws SQLException {
    Statement stmt = dbConn.createStatement();
    stmt.execute("insert into DimConf(ConfDesc) values ('" + confKey + "');");
    dbConn.commit();
    ResultSet rs = stmt.executeQuery("select ConfID from DimConf where ConfDesc = '" + 
        confKey + "'");
    short id = -1;
    while(rs.next()) {
      id = rs.getShort(1);
      break;
    }
    rs.close();
    stmt.close();
    LOG.debug("Added a new confKey " + confKey + " and assigned it ID " + id);
    return id;
  }
  
}
