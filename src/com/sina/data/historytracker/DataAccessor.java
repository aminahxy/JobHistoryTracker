package com.sina.data.historytracker;

import java.io.IOException;
import java.io.Reader;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.ibatis.common.resources.Resources;
import com.ibatis.sqlmap.client.SqlMapClient;
import com.ibatis.sqlmap.client.SqlMapClientBuilder;

public class DataAccessor {
  private static SqlMapClient sqlMapper;
  public static final Logger LOG = Logger.getLogger(DataAccessor.class.getName());

  static {
    try {
      Reader reader = Resources.getResourceAsReader("sqlmap_cfg.xml");
      sqlMapper = SqlMapClientBuilder.buildSqlMapClient(reader);
      reader.close();
    } catch (IOException e) {
      // Fail fast.
      throw new RuntimeException(
          "Something bad happened while building the SqlMapClient instance."
              + e, e);
    }
  }

  private DataAccessor() { }
  
  public static void commit() throws SQLException {
    sqlMapper.commitTransaction();
  }
  
  public static void startBath() throws SQLException {
    sqlMapper.startBatch();
  }
  
  public static int execBath() throws SQLException {
    return sqlMapper.executeBatch();
  }
  
  public static List query(final String sqlId, Map paramMap) {
    try {
      return sqlMapper.queryForList(sqlId, paramMap);
    } catch (SQLException e) {
      LOG.error("Error", e);
      return null;
    }
  }
  
  public static Object insert(final String sqlId, Map paramMap) {
    try {
      return sqlMapper.insert(sqlId, paramMap);
    } catch (SQLException e) {
      LOG.error("Error", e);
      return null;
    }
  }
}
