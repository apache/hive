package org.apache.hadoop.hive.ql.cube.parse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class PartitionResolver implements ContextRewriter {

  public PartitionResolver(Configuration conf) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) {
    Map<String, List<String>> partitionColMap = new HashMap<String,
        List<String>>();
    /*Date fromDate = cubeql.getFromDate();
    Date toDate = cubeql.getToDate();

    //resolve summary table names, applicable only if query is on fact table

    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);

    UpdatePeriod interval = null;
    for (CubeFactTable fact : cubeql.getFactTables()) {
      while ((interval = CubeFactTable.maxIntervalInRange(fromDate, toDate,
          fact.getUpdatePeriods())) != null) {
        List<String> partitions = fact.getPartitions(fromDate, toDate,
            interval);
        if (partitions != null) {
          partitionColMap.put(MetastoreUtil.getVirtualFactTableName(
              fact.getName(), interval), partitions);
          // Advance from date
          cal.setTime(fromDate);
          cal.roll(interval.calendarField(), partitions.size());
          fromDate = cal.getTime();
        }
      }
    }
    for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      partitionColMap.put(MetastoreUtil.getVirtualDimTableName(
          dim.getName()), dim.getPartitions());
    }

    // set partition cols map in cubeql
    //TODO
     *
     */
  }

}
