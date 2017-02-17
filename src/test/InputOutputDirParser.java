package test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.sina.data.historytracker.Constants;
import com.sina.data.historytracker.GlobExpander;
import com.sina.data.historytracker.Manager;

public class InputOutputDirParser {

  static{
    Configuration.addDefaultResource(Constants.CONFFILE);
    PropertyConfigurator.configure("conf/log4j.properties");
  }
  
  public static final Logger LOG = Logger.getLogger(Manager.class.getName());
  
  public static void main(String[] args) throws Exception {
    Connection conn = null;
    Configuration conf = new Configuration();
    Statement stmt = null;
    String url = conf.get("history.tracker.db.connection");
    if (url == null || url.equals("")) {
      LOG.error("Need a connection specified in conf file!");
      return;
    }
    String dbUser = conf.get("history.tracker.db.user");
    String dbPasswd = conf.get("history.tracker.db.password");
    try {
      conn = DriverManager.getConnection(url, dbUser, dbPasswd);
      conn.setAutoCommit(false);
      stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("select a.JobID,b.ConfValue from JobInput a,JobConf b where instr(a.inputdir,'{') and b.JobID=a.JobID and b.ConfID=306");
      while(rs.next()) {
        long id = rs.getLong(1);
        String confV = rs.getString(2);
        StringBuilder sb = new StringBuilder("insert into JobInput (JobID, InputDir) values ");
        for (String s : splitJobDir(confV)) {
          sb.append("('").append(id).append("','").append(s).append("'), ");
        }
        System.out.println(sb.substring(0, sb.length()-2)+';');
//        stmt.execute(sb.toString());
//        conn.commit();
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (stmt != null)
        stmt.close();
      if (conn != null)
        conn.close();
    }
  }
  
  static List<String> splitJobDir(String in) {
    List<String> result = new ArrayList<String>();
    String[] raw = StringUtils.split(in);
    for (String r : raw) {
      Path srcPath = new Path(r);
      try {
        List<String> dirs = GlobExpander.expand(srcPath.toUri().getPath());
        for (String d : dirs) {
          Path t = null;
          try {
            t = new Path(d);
          } catch (Exception e) {
            System.err.println(in);
          }
          if (t.getName().equals("*"))
            t = t.getParent();
          result.add(t.toUri().getPath());
        }
      } catch (IOException e) {
        continue;
      }
    }
    return result;
  }
}
