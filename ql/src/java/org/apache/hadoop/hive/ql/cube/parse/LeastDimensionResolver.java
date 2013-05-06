package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LeastDimensionResolver implements ContextRewriter {

  public LeastDimensionResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql)
      throws SemanticException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables().isEmpty()) {
      Map<CubeFactTable, Integer> dimWeightMap =
          new HashMap<CubeFactTable, Integer>();

      for (CubeFactTable fact : cubeql.getCandidateFactTables()) {
        dimWeightMap.put(fact, getDimensionWeight(cubeql, fact));
     }

     int minWeight = Collections.min(dimWeightMap.values());

     for (Iterator<CubeFactTable> i =
         cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
       CubeFactTable fact = i.next();
       if (dimWeightMap.get(fact) > minWeight) {
         System.out.println("Removing fact:" + fact +
             " from candidate fact tables as it has more dimension weight:"
             +  dimWeightMap.get(fact) + " minimum:"
             + minWeight);
         i.remove();
       }
     }
    }
  }

  private Integer getDimensionWeight(CubeQueryContext cubeql, CubeFactTable fact) {
    //TODO get the dimension weight associated with the fact wrt query
    return 0;
  }

}
