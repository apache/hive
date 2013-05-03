package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.cube.metadata.UpdatePeriod;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LeastPartitionResolver implements ContextRewriter {

  public LeastPartitionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    if (cubeql.getCube() != null) {
      Map<CubeFactTable, Integer> numPartitionsMap =
          new HashMap<CubeFactTable, Integer>();

      for (CubeFactTable fact : cubeql.getCandidateFactTables()) {
         numPartitionsMap.put(fact, getTotalPartitions(
             cubeql.getFactPartitionMap().get(fact)));
      }

      int minPartitions = Collections.min(numPartitionsMap.values());

      for (Iterator<CubeFactTable> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CubeFactTable fact = i.next();
        if (numPartitionsMap.get(fact) > minPartitions) {
          System.out.println("Removing fact:" + fact +
              " from candidate fact tables as it requires more partitions to" +
              " be queried:" +  numPartitionsMap.get(fact) + " minimum:"
              + minPartitions);
          i.remove();
        }
      }
    }
  }

  private int getTotalPartitions(Map<UpdatePeriod, List<String>> map) {
    int count = 0;
    for (List<String> parts : map.values()) {
      count += parts.size();
    }
    return count;
  }
}
