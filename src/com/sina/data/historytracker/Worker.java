package com.sina.data.historytracker;

import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.JobHistory.JobInfo;
import org.apache.hadoop.mapred.JobHistory.Keys;
import org.apache.hadoop.mapred.JobHistory.TaskAttempt;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class Worker extends TimerTask{
  
  static{
    Configuration.addDefaultResource(Constants.CONFFILE);
    PropertyConfigurator.configure("conf/log4j.properties");
  }
  
  public static final Logger LOG = Logger.getLogger(Worker.class.getName());
  LinkedBlockingQueue<String> queue = null;
  private Configuration conf = null;
  private Connection conn = null;
  private Statement stmt = null;
  private PreparedStatement jobinfoPst = null;
  private PreparedStatement jobcounterPst = null;
  private PreparedStatement taskinfoPst = null;
  private PreparedStatement taskcounterPst = null;
  private PreparedStatement jobinputPst = null;
  private PreparedStatement joboutputPst = null;
  private PreparedStatement jobconfPst = null;
  private DimConfManager dimConf = null;
  private DimCounterManager dimCounter = null;
  
  Comparator<JobHistory.Task> cMap = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - t2.getLong(Keys.START_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  
  Comparator<JobHistory.Task> cReduce = new Comparator<JobHistory.Task>(){
    public int compare(JobHistory.Task t1, JobHistory.Task t2){
      long l1 = t1.getLong(Keys.FINISH_TIME) - 
                t1.getLong(Keys.START_TIME); 
      long l2 = t2.getLong(Keys.FINISH_TIME) - 
                t2.getLong(Keys.START_TIME);
      return (l2<l1 ? -1 : (l2==l1 ? 0 : 1));
    }
  }; 
  
  public Worker(Configuration c, LinkedBlockingQueue<String> workQ) throws Exception {
    this.conf = c;
    this.queue = workQ;
    getDBConn();
    dimConf = new DimConfManager(conf, conn);
    dimCounter = new DimCounterManager(conf, conn);
  }
  
  private Connection getDBConn() throws Exception {
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
    } catch (Exception e) {
      LOG.error("Error", e);
      throw e;
    }
    String url = conf.get("history.tracker.db.connection");
    if (url == null || url.equals("")) {
      LOG.error("Need a connection specified in conf file!");
      return null;
    }
    String dbUser = conf.get("history.tracker.db.user");
    String dbPasswd = conf.get("history.tracker.db.password");
    try {
      conn = DriverManager.getConnection(url, dbUser, dbPasswd);
      conn.setAutoCommit(false);
      stmt = conn.createStatement();
      jobinfoPst = conn.prepareStatement("insert into JobInfo " +
      		"(JobID,JobName,StartTime,EndTime,User,SubmitAddress," +
      		"Status,SetupStatus,CleanupStatus,ScheduleNodeID,ScheduleWFID,HiveID) " +
      		"values(?,?,?,?,?,?,?,?,?,?,?,?)");
      taskinfoPst = conn.prepareStatement("insert into TaskInfo " +
      		"(Taskid,JobID,Status,StartTime,EndTime,Ismap) " +
      		"values(?,?,?,?,?,?)");
      jobcounterPst = conn.prepareStatement("insert into JobCounter" +
      		"(JobID,CounterID,CounterValue) " +
      		"values(?,?,?)");
      taskcounterPst = conn.prepareStatement("insert into TaskCounter" +
      		"(TaskID,CounterID,CounterValue) " +
      		"values(?,?,?)");
      jobinputPst = conn.prepareStatement("insert into JobInput(JobID,InputDir,Columns) " +
      		"values(?,?,?)");
      joboutputPst = conn.prepareStatement("insert into JobOutput(JobID,OutputDir) " +
          "values(?,?)");
      jobconfPst = conn.prepareStatement("insert into JobConf(JobID,ConfID,ConfValue) " +
      		"values(?,?,?)");
    } catch (SQLException e) {
      LOG.error("Error", e);
      return null;
    }
    return conn;
  }

  @Override
  public void run() {
    try {
      long ts = System.currentTimeMillis();
      String historyLogName = queue.poll();
      if (historyLogName == null || historyLogName.equals("")) {
        return;
      }
      // job_201208061840_93717_1344787539481_sinadata_VgongyiUserFeatrue%5Fprovince
      // job_201208061840_93717_conf.xml
      // 0123456789012345678901234567890
      int confIndex = historyLogName.indexOf('_', historyLogName.lastIndexOf('/')+18);
      String confFileName = historyLogName.substring(0, confIndex)
          + "_conf.xml";

      String jobid = jobIdNameFromLogFileName(historyLogName);
      JobInfo jobInfo = new JobHistory.JobInfo(jobid);
      DefaultJobHistoryParser.parseJobTasks(historyLogName, jobInfo,
          FileSystem.getLocal(conf));
      Configuration c = new Configuration(false);
      c.addResource(new Path(confFileName));
      try {
        writeToDB(jobInfo, c);
//        writeToDBUSingIbatis(jobInfo, c);
      } catch (Exception e) {
        LOG.error("Error", e);
      }
      String jobIDString = confFileName.substring(confFileName.lastIndexOf('/') + 1, 
          confFileName.lastIndexOf('_'));
      LOG.info("Finished " + jobIDString + " using " + 
                    (System.currentTimeMillis() - ts) + " ms, remaining " + queue.size());
    } catch (Exception e) {
      LOG.error("Error", e);
    }
  }
  
  private void writeToDB(JobInfo job, Configuration jobConf) throws SQLException {
    String jobid = job.get(Keys.JOBID);
    String username = job.get(Keys.USER);// user name
    String jobname = job.get(Keys.JOBNAME);// job name
    long submitTime = job.getLong(Keys.SUBMIT_TIME);// submit time in ms
    long launchTime = job.getLong(Keys.LAUNCH_TIME); 
    long finishTime = job.getLong(Keys.FINISH_TIME);
    String status = job.get(Keys.JOB_STATUS);
    String setupStatus = "";
    String cleanupStatus = "";
    String ScheduleNodeID = jobConf.get("HADOOPENV_STEP_INS_ID");
    String ScheduleWFID = jobConf.get("HADOOPENV_WF_INS_ID");
    String HiveID = jobConf.get("hive.query.id", "");
    String SubmitAddress = jobConf.get("mapreduce.job.submithostaddress", "");
    jobinfoPst.setString(1, jobid);
    int jobnamesize = jobname.length() > 255 ? 255 : jobname.length();
    jobinfoPst.setString(2, jobname.substring(0, jobnamesize));
    jobinfoPst.setTimestamp(3, new Timestamp(submitTime));
    jobinfoPst.setTimestamp(4, new Timestamp(finishTime));
    jobinfoPst.setString(5, username);
    jobinfoPst.setString(6, SubmitAddress);
    jobinfoPst.setString(7, status);
    jobinfoPst.setString(8, setupStatus);
    jobinfoPst.setString(9, cleanupStatus);
    jobinfoPst.setString(10, ScheduleNodeID);
    jobinfoPst.setString(11, ScheduleWFID);
    jobinfoPst.setString(12, HiveID);
    jobinfoPst.execute();
    conn.commit();
    ResultSet rs = stmt.executeQuery("select ID from JobInfo where JobID='" + jobid +"'");
    int jobDBID = -1;
    if(rs.next()) {
      jobDBID = rs.getInt(1);
    } else {
      throw new SQLException("Failed to retrieve ID for " + jobid);
    }
    
    List<String> input = new ArrayList<String>();
    String jobinputs = jobConf.get("mapred.input.dir");
    if (jobinputs != null && !jobinputs.equals("")) {
      input.addAll(splitJobDir(jobinputs));
    }
    String multiInputs = jobConf.get("mapred.input.dir.mappers");
    if (multiInputs != null && !multiInputs.equals("")) {
      String[] mInputs = StringUtils.split(multiInputs);
      for(String i : mInputs) {
        String a[] = i.split(";");
        List<String> tempL = splitJobDir(a[0]);
        input.addAll(tempL);
      }
    }
    if (input != null && input.size() > 0) {
      for (String f : input) {
        jobinputPst.setInt(1, jobDBID);
        jobinputPst.setString(2, f);
        jobinputPst.setString(3, "");
        jobinputPst.addBatch();
      }
      try {
        jobinputPst.executeBatch();
      } catch (BatchUpdateException e1) {
        if (!e1.getMessage().contains("Duplicate entry")) {
          throw e1;
        }
      }
    }
    
    String joboutputs = jobConf.get("mapred.output.dir");
    if (joboutputs != null && !joboutputs.equals("")) {
      List<String> output = splitJobDir(joboutputs);
      for (String f : output) {
        joboutputPst.setInt(1, jobDBID);
        joboutputPst.setString(2, f);
        joboutputPst.addBatch();
      }
      try {
        joboutputPst.executeBatch();
      } catch (BatchUpdateException e1) {
        if (!e1.getMessage().contains("Duplicate entry")) {
          throw e1;
        }
      }
    }
    
    // write conf to DB
    for(Entry<String, String> e : jobConf) {
      short id = dimConf.getID(e.getKey());
      jobconfPst.setInt(1, jobDBID);
      jobconfPst.setShort(2, id);
      String value = e.getValue();
      int size = value.length() > 10230 ? 10230 : value.length();
      jobconfPst.setString(3, value.substring(0, size));
      jobconfPst.addBatch();
    }
    jobconfPst.executeBatch();
    
    ArrayList<JobHistory.TaskAttempt> mapTasks = new ArrayList<JobHistory.TaskAttempt>(); 
    ArrayList<JobHistory.TaskAttempt> reduceTasks = new ArrayList<JobHistory.TaskAttempt>(); 
    
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    for (JobHistory.Task t : tasks.values()) {
      t.get(Keys.HOSTNAME);// which machine run this attempt
      Map<String, TaskAttempt> attempts = t.getTaskAttempts();
      for (TaskAttempt attempt : attempts.values()) {
        long taskStartTime = attempt.getLong(Keys.START_TIME);
        long taskEndTime = attempt.getLong(Keys.FINISH_TIME);
        String taskStatus = attempt.get(Keys.TASK_STATUS);
        String attemptID = attempt.get(Keys.TASK_ATTEMPT_ID);
        String taskType = attempt.get(Keys.TASK_TYPE);
        taskinfoPst.setString(1, attemptID);
        taskinfoPst.setInt(2, jobDBID);
        taskinfoPst.setString(3, taskStatus);
        if (taskStartTime != 0) {
          taskinfoPst.setTimestamp(4, new Timestamp(taskStartTime));
        }
        if (taskEndTime != 0) {
          taskinfoPst.setTimestamp(5, new Timestamp(taskEndTime));
        }
        taskinfoPst.setShort(6, (short) (taskType.equals("MAP") ? 1 : 0));
        taskinfoPst.addBatch();
        
        if (taskType.equals("MAP")) {
          mapTasks.add(attempt);
        } else if (taskType.equals("REDUCE")) {
          reduceTasks.add(attempt);
        }
      }
    } // end for 
    taskinfoPst.executeBatch();
    conn.commit();
    writeTaskCounters(jobid, mapTasks, cMap);
    writeTaskCounters(jobid, reduceTasks, cReduce);
    taskcounterPst.executeBatch();
    
    try {
      Counters totalCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.COUNTERS));
        Counters mapCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.MAP_COUNTERS));
        Counters reduceCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.REDUCE_COUNTERS));
        
      if (totalCounters != null) {
        for (String groupName : totalCounters.getGroupNames()) {
          Counters.Group totalGroup = totalCounters.getGroup(groupName);

          Iterator<Counters.Counter> ctrItr = totalGroup.iterator();
          while (ctrItr.hasNext()) {
            Counters.Counter counter = ctrItr.next();
            String name = groupName + "#" + counter.getDisplayName();
            short id = dimCounter.getID(name);
            long totalValue = counter.getCounter();
            jobcounterPst.setInt(1, jobDBID);
            jobcounterPst.setShort(2, id);
            jobcounterPst.setLong(3, totalValue);
            jobcounterPst.addBatch();
          }
        }
        jobcounterPst.executeBatch();
      }
    } catch (ParseException e) {
      LOG.error("Error", e);
    }
    
    conn.commit();
  }
  
  static List<String> splitJobDir(String in) {
    List<String> result = new ArrayList<String>();
    String[] raw = StringUtils.split(in);
    for (String r : raw) {
      Path srcPath = new Path(r);
      try {
        List<String> dirs = GlobExpander.expand(srcPath.toUri().getPath());
        for (String d : dirs) {
          Path t = new Path(d);
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

  private int writeTaskCounters(String jobid, ArrayList<JobHistory.TaskAttempt> tasks, 
      Comparator<JobHistory.Task> comp)
      throws SQLException {
    ResultSet rs;
    int worstCount = 10;
    int i = 0;
    Collections.sort(tasks, comp);
    for (i = 0; i < worstCount && i < tasks.size(); i++) {
      TaskAttempt attempt = tasks.get(i);
      String attemptID = attempt.get(Keys.TASK_ATTEMPT_ID);
      rs = stmt.executeQuery("select ID from TaskInfo where TaskID='" + attemptID +"'");
      int taskDBID = -1;
      if(rs.next()) {
        taskDBID = rs.getInt(1);
      } else {
        throw new SQLException("Failed to retrieve ID for " + jobid);
      }

      Counters counters;
      try {
        counters = Counters.fromEscapedCompactString(attempt
            .get(Keys.COUNTERS));
        if (counters != null) {
          for (String groupName : counters.getGroupNames()) {
            Counters.Group group = counters.getGroup(groupName);
            Iterator<Counters.Counter> ctrItr = group.iterator();
            while(ctrItr.hasNext()) {
              Counters.Counter counter = ctrItr.next();
              String name = groupName + "#" + counter.getDisplayName();
              short id = dimCounter.getID(name);
              long totalValue = counter.getCounter();
              taskcounterPst.setInt(1, taskDBID);
              taskcounterPst.setShort(2, id);
              taskcounterPst.setLong(3, totalValue);
              taskcounterPst.addBatch();
            }
          }
        }
      } catch (ParseException e) {
        LOG.error("Error", e);
      }
    }
    return i;
  }

  private void writeToDBUSingIbatis(JobInfo job, Configuration jobConf) throws SQLException {
    HashMap<String, Object> data = new HashMap<String, Object>();
    String jobid = job.get(Keys.JOBID);
    String username = job.get(Keys.USER);// user name
    String jobname = job.get(Keys.JOBNAME);// job name
    long submitTime = job.getLong(Keys.SUBMIT_TIME);// submit time in ms
    long launchTime = job.getLong(Keys.LAUNCH_TIME); 
    long finishTime = job.getLong(Keys.FINISH_TIME);
    String status = job.get(Keys.JOB_STATUS);
    String setupStatus = "";
    String cleanupStatus = "";
    String ScheduleNodeID = "";
    String ScheduleWFID = "";
    String HiveID = jobConf.get("hive.query.id", "");
    String SubmitAddress = jobConf.get("mapreduce.job.submithostaddress", "");
    data.put("JobID", jobid);
    int jobnamesize = jobname.length() > 255 ? 255 : jobname.length();
    data.put("JobName", jobname.substring(0, jobnamesize));
    data.put("StartTime", new Timestamp(submitTime));
    data.put("EndTime", new Timestamp(finishTime));
    data.put("User", username);
    data.put("SubmitAddress", SubmitAddress);
    data.put("Status", status);
    data.put("SetupStatus", setupStatus);
    data.put("CleanupStatus", cleanupStatus);
    data.put("ScheduleNodeID", ScheduleNodeID);
    data.put("ScheduleWFID", ScheduleWFID);
    data.put("HiveID", HiveID);
    DataAccessor.insert("sqls.insertjobinfo", data);
    
    data.clear();
    data.put("JobID", jobid);
    List result = DataAccessor.query("sqls.getJobDBID", data);
    int jobDBID = -1;
    if (result != null && result.size() > 0) {
      Map<String, Object> t = (Map<String, Object>) result.get(result.size()-1);
      jobDBID = (Integer) t.get("id");
    } else {
      throw new SQLException("Failed to retrieve ID for " + jobid);
    }
    
    ArrayList<JobHistory.TaskAttempt> mapTasks = new ArrayList<JobHistory.TaskAttempt>(); 
    ArrayList<JobHistory.TaskAttempt> reduceTasks = new ArrayList<JobHistory.TaskAttempt>(); 
    
    DataAccessor.startBath();
    Map<String, JobHistory.Task> tasks = job.getAllTasks();
    for (JobHistory.Task t : tasks.values()) {
      t.get(Keys.HOSTNAME);// which machine run this attempt
      Map<String, TaskAttempt> attempts = t.getTaskAttempts();
      for (TaskAttempt attempt : attempts.values()) {
        data.clear();
        long taskStartTime = attempt.getLong(Keys.START_TIME);
        long taskEndTime = attempt.getLong(Keys.FINISH_TIME);
        String taskStatus = attempt.get(Keys.TASK_STATUS);
        String attemptID = attempt.get(Keys.TASK_ATTEMPT_ID);
        String taskType = attempt.get(Keys.TASK_TYPE);
        data.put("Taskid", attemptID);
        data.put("JobID", jobDBID);
        data.put("Status", taskStatus);
        if (taskStartTime != 0) {
          data.put("StartTime", new Timestamp(taskStartTime));
        }
        if (taskEndTime != 0) {
          data.put("EndTime", new Timestamp(taskStartTime));
        }
        data.put("Ismap", (short) (taskType.equals("MAP") ? 1 : 0));
        DataAccessor.insert("sqls.inserttaskinfo", data);
        
        if (taskType.equals("MAP")) {
          mapTasks.add(attempt);
        } else if (taskType.equals("REDUCE")) {
          reduceTasks.add(attempt);
        }
      }
    } // end for 
    DataAccessor.execBath();
    DataAccessor.startBath();
    writeTaskCountersUSingIbatis(jobid, mapTasks, cMap);
    writeTaskCountersUSingIbatis(jobid, reduceTasks, cReduce);
    DataAccessor.execBath();
    
    try {
      Counters totalCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.COUNTERS));
        Counters mapCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.MAP_COUNTERS));
        Counters reduceCounters = 
          Counters.fromEscapedCompactString(job.get(Keys.REDUCE_COUNTERS));
        
      if (totalCounters != null) {
        DataAccessor.startBath();
        for (String groupName : totalCounters.getGroupNames()) {
          Counters.Group totalGroup = totalCounters.getGroup(groupName);
          Iterator<Counters.Counter> ctrItr = totalGroup.iterator();
          while (ctrItr.hasNext()) {
            data.clear();
            Counters.Counter counter = ctrItr.next();
            String name = groupName + "#" + counter.getDisplayName();
            short id = dimCounter.getID(name);
            long totalValue = counter.getCounter();
            data.put("JobID", jobDBID);
            data.put("CounterID", id);
            data.put("CounterValue", totalValue);
            DataAccessor.insert("sqls.insertjobcounter", data);
          }
        }
        DataAccessor.execBath();
      }
    } catch (ParseException e) {
      LOG.error("Error", e);
    }
    
    String jobinputs = jobConf.get("mapred.input.dir");
    if (jobinputs != null && !jobinputs.equals("")) {
      String input[] = jobinputs.split(",");
      DataAccessor.startBath();
      for (String f : input) {
        data.clear();
        data.put("JobID", jobDBID);
        data.put("InputDir", f);
        data.put("Columns", "");
        DataAccessor.insert("sqls.insertjobinput", data);
      }
      try {
        DataAccessor.execBath();
      } catch (BatchUpdateException e1) {
        if (!e1.getMessage().contains("Duplicate entry")) {
          throw e1;
        }
      }
    }
    
    String joboutputs = jobConf.get("mapred.output.dir");
    if (joboutputs != null && !joboutputs.equals("")) {
      String output[] = joboutputs.split(",");
      DataAccessor.startBath();
      for (String f : output) {
        data.clear();
        data.put("JobID", jobDBID);
        data.put("OutputDir", f);
        DataAccessor.insert("sqls.insertjoboutput", data);
      }
      try {
        DataAccessor.execBath();
      } catch (BatchUpdateException e1) {
        if (!e1.getMessage().contains("Duplicate entry")) {
          throw e1;
        }
      }
    }
    
    // write conf to DB
    DataAccessor.startBath();
    for(Entry<String, String> e : jobConf) {
      short id = dimConf.getID(e.getKey());
      data.clear();
      data.put("JobID", jobDBID);
      data.put("ConfID", id);
      String value = e.getValue();
      int size = value.length() > 1023 ? 1023 : value.length();
      data.put("ConfValue", value.substring(0, size));
      DataAccessor.insert("sqls.insertjobconf", data);
    }
    DataAccessor.execBath();
  }
  
  private int writeTaskCountersUSingIbatis(String jobid, ArrayList<JobHistory.TaskAttempt> tasks, 
      Comparator<JobHistory.Task> comp)
      throws SQLException {
    int worstCount = 10;
    int i = 0;
    Collections.sort(tasks, comp);
    for (i = 0; i < worstCount && i < tasks.size(); i++) {
      TaskAttempt attempt = tasks.get(i);
      HashMap<String, Object> data = new HashMap<String, Object>();
      data.put("TaskID", attempt);
      List result = DataAccessor.query("sqls.getTaskDBID", data);
      int taskDBID = -1;
      if (result != null && result.size() > 0) {
        Map<String, Object> t = (Map<String, Object>) result.get(result.size()-1);
        taskDBID = (Integer) t.get("id");
      } else {
        throw new SQLException("Failed to retrieve ID for " + jobid);
      }

      Counters counters;
      try {
        counters = Counters.fromEscapedCompactString(attempt
            .get(Keys.COUNTERS));
        if (counters != null) {
          for (String groupName : counters.getGroupNames()) {
            Counters.Group group = counters.getGroup(groupName);
            Iterator<Counters.Counter> ctrItr = group.iterator();
            while(ctrItr.hasNext()) {
              data.clear();
              Counters.Counter counter = ctrItr.next();
              String name = groupName + "#" + counter.getDisplayName();
              short id = dimCounter.getID(name);
              long totalValue = counter.getCounter();
              data.put("TaskID", taskDBID);
              data.put("CounterID", id);
              data.put("CounterValue", totalValue);
              DataAccessor.insert("sqls.inserttaskcounter", data);
            }
          }
        }
      } catch (ParseException e) {
        LOG.error("Error", e);
      }
    }
    return i;
  }
  
  public void close() {
    try {
      stmt.close();
      jobinfoPst.close();
      jobcounterPst.close();
      taskinfoPst.close();
      taskcounterPst.close();
      jobinputPst.close();
      joboutputPst.close();
      jobconfPst.close();
      conn.close();
    } catch (SQLException e) {
      LOG.error("Error", e);
    }
  }
  
  /**
   * copied from JobHistory.java
   * @param logFileName
   * @return
   */
  static String jobIdNameFromLogFileName(String logFileName) {
    String[] jobDetails = logFileName.split("_");
    return jobDetails[0] + "_" + jobDetails[1] + "_" + jobDetails[2];
  }
  
  public static void main(String args[]) throws Exception {
//    LinkedBlockingQueue<String> workQ = new LinkedBlockingQueue<String>();
//    Configuration c = new Configuration();
//    Worker w = new Worker(c, workQ);
//    Thread t = new Thread(w);
//    t.start();
////    workQ.add("/host/workspace/HadoopMetaManager/jobhistorytracker/testdir1/001509/job_201208061840_1509757_1351021752392_optimus_Generate+Cookie2ITInfo+job%2C+type+%3D+MERGE%5FWEIBO%5FIT%5F");
//    workQ.add("/host/workspace/HadoopMetaManager/jobhistorytracker/testdir1/001516/job_201208061840_1516363_1351045836869_sinadata_combinebehavior%5Fyz.jar");
//    Thread.sleep(1000);
//    w.close();
//    t.interrupt();
    List<String> l = splitJobDir("hdfs://nn1.hadoop.data.sina.com.cn/file/tblog/behavior/14000003/{2012/05/17\\,2012/05/18\\,2012/05/19\\,2012/05/20\\,2012/05/21\\,2012/05/22\\,2012/05/23\\,2012/05/24\\,2012/05/25\\,2012/05/26\\,2012/05/27\\,2012/05/28\\,2012/05/29\\,2012/05/30\\,2012/05/31\\,2012/06/01\\,2012/06/02\\,2012/06/03\\,2012/06/04\\,2012/06/05\\,2012/06/06\\,2012/06/07\\,2012/06/08\\,2012/06/09\\,2012/06/10\\,2012/06/11\\,2012/06/12\\,2012/06/13\\,2012/06/14\\,2012/06/15\\,2012/06/16\\,2012/06/17\\,2012/06/18\\,2012/06/19\\,2012/06/20\\,2012/06/21\\,2012/06/22\\,2012/06/23\\,2012/06/24\\,2012/06/25\\,2012/06/26\\,2012/06/27\\,2012/06/28\\,2012/06/29\\,2012/06/30}/LOG_INFO_STORE.log.lzo");
    for (String s : l)
      System.out.println(s);
  }
}
