package test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hsqldb.lib.StringUtil;

import com.sina.data.historytracker.Constants;
import com.sina.data.historytracker.DataAccessor;
import com.sina.data.historytracker.GlobExpander;

public class Tester {

  static{
    Configuration.addDefaultResource(Constants.CONFFILE);
    PropertyConfigurator.configure("conf/log4j.properties");
  }
  
  public static void main1(String[] args) {
    String abc = "1234567890";
    System.out.println(abc.substring(0,20));
  }
  
  public static void main(String[] args) throws IOException {
    String path = "/file/scribe/wb_ad_cpmimp/2012/07/18";
    Path srcPath = new Path(path);
    int depthToStop = 0;
    String components[] = StringUtils.split(path, StringUtils.ESCAPE_CHAR, '/');
    for(int i = 0; i < components.length && isEffectiveComponent(components[i]); ++i) {
      ++depthToStop;
    }
    while(srcPath != null && srcPath.depth() > depthToStop - 2) {
      if (srcPath.getName().indexOf('*') != -1 || 
          srcPath.getName().indexOf('?') != -1) {
      }
      System.out.println(srcPath.toUri().toString());
      srcPath = srcPath.getParent();
    }
  }

  private static boolean isEffectiveComponent(String str) {
    try {
      Long.parseLong(str);
    } catch (NumberFormatException e) {
      if (str.startsWith("dt="))
        return false;
      return true;
    }
    return false;
  }
}
