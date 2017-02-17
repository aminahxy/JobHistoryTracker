package test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

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

public class HiveTester {

  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {
    SessionState.initHiveLog4j();
    HiveConf hc = new HiveConf();
    hc.addResource("hive-default.xml");
    CliSessionState css = new CliSessionState(new HiveConf(SessionState.class));
    css.out = System.out;
    css.err = System.err;
    SessionState.start(css);
    Driver d = new Driver(hc);
    String command = "INSERT OVERWRITE TABLE page_view_stat PARTITION (dt='20121101',visit_type='WAP') SELECT uid,loc,type, sum(cnt) from ad_algo_visit_type_pv where visit_type='WAP' GROUP BY uid,loc,type";
//    String command = "show tables";
    d.compile(command);
    QueryPlan qp = d.getPlan();
    if (qp == null) {
      System.out.println("No plans found");
      return;
    }
    HashSet<ReadEntity> inputs = qp.getInputs();
    for (ReadEntity re : inputs) {
      Table t = re.getT();
      t.getTableName();
      System.out.println("table " + t.getTableName());
      Partition p = re.getP();
      System.out.println("partition " + p.getName());
    }
    HashSet<WriteEntity> outputs = qp.getOutputs();
    for (WriteEntity we : outputs) {
      System.out.println(we.getLocation());
      System.out.println(we.getType());
    }
    System.out.println(qp.getFetchTask());
    ArrayList<Task<? extends Serializable>> roots = qp.getRootTasks();
    //TODO: should go recursively
    for (Task t : roots) {
      if (t instanceof MapRedTask) {
        MapRedTask mrt = (MapRedTask) t;
        mrt.getChildTasks();
        LinkedHashMap<String, PartitionDesc> aliasToPart = mrt.getWork().getAliasToPartnInfo();
        LinkedHashMap<String, Operator<? extends Serializable>> aliasToWork = 
            mrt.getWork().getAliasToWork();
        for (Entry<String, Operator<? extends Serializable>> e : aliasToWork.entrySet()) {
          String alias = e.getKey();
          Operator o = e.getValue();
          if (o instanceof TableScanOperator) {
            TableScanOperator tso = (TableScanOperator) o;
            // TODO: save them
            PartitionDesc p = aliasToPart.get(alias);
            System.out.println(p.getTableName());// table name
            ArrayList<Integer> columns = tso.getNeededColumnIDs();//column ids
            System.out.println(columns);
          }
        }
      }
    }
//    System.out.println(qp.toString());
//    d.execute();
    ArrayList<String> res = new ArrayList<String>();
    while (d.getResults(res)) {
      for (String r : res) {
        System.out.println(r);
      }
      res.clear();
      if (System.out.checkError()) {
        break;
      }
    }
    d.close();
    
  }

}
