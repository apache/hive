package org.apache.hadoop.hive.ql.cube.parse;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class PartitionResolver implements ContextRewriter {

  public PartitionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    Map<CubeFactTable, Map<UpdatePeriod, List<String>>> factPartitionMap =
        new HashMap<CubeFactTable, Map<UpdatePeriod, List<String>>>();
    Date fromDate = cubeql.getFromDate();
    Date toDate = cubeql.getToDate();

    Calendar cal = Calendar.getInstance();
    cal.setTime(fromDate);

    for (CubeFactTable fact : cubeql.getFactTables()) {
      Map<UpdatePeriod, List<String>> partitionColMap =
          new HashMap<UpdatePeriod, List<String>>();
      factPartitionMap.put(fact, partitionColMap);
      getPartitions(fact, fromDate, toDate, partitionColMap);
    }

    /*for (CubeDimensionTable dim : cubeql.getDimensionTables()) {
      partitionColMap.put(MetastoreUtil.getVirtualDimTableName(
          dim.getName()), dim.getPartitions());
    }*/

    // set partition cols map in cubeql
    cubeql.setFactPartitionMap(factPartitionMap);
  }

  void getPartitions(CubeFactTable fact, Date fromDate, Date toDate,
      Map<UpdatePeriod, List<String>> partitionColMap)
          throws SemanticException {
    System.out.println("getPartitions fromDate:" + fromDate + " toDate:" + toDate);
    if (fromDate.equals(toDate) || fromDate.after(toDate)) {
      return;
    }

    UpdatePeriod interval = fact.maxIntervalInRange(fromDate, toDate);
    if (interval == null) {
      throw new SemanticException("Could not find a partition for given range:"
        + fromDate + "-" + toDate);
    }

    System.out.println("fact: " + fact.getName() + " max interval:" + interval);
    Date ceilFromDate = DateUtils.getCeilDate(fromDate, interval);
    Date floorToDate = DateUtils.getFloorDate(toDate, interval);
    List<String> partitions = fact.getPartitions(ceilFromDate, floorToDate,
          interval);
    if (partitions != null) {
      List<String> parts = partitionColMap.get(interval);
      if (parts == null) {
        parts = new ArrayList<String>();
        partitionColMap.put(interval, parts);
      }
      parts.addAll(partitions);
    }
    System.out.println("ceilFromDate:" + ceilFromDate);
    System.out.println("floorToDate:" + floorToDate);
    getPartitions(fact, fromDate, ceilFromDate, partitionColMap);
    getPartitions(fact, floorToDate, toDate, partitionColMap);
  }
}
