package org.apache.hadoop.hive.ql.cube.parse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.cube.metadata.CubeFactTable;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class LightestFactResolver implements ContextRewriter {
  public static final Log LOG = LogFactory.getLog(
      LightestFactResolver.class.getName());

  public LightestFactResolver(Configuration conf) {
  }

  @Override
  public void rewriteContext(CubeQueryContext cubeql) throws SemanticException {
    if (cubeql.getCube() != null && !cubeql.getCandidateFactTables()
        .isEmpty()) {
      Map<CubeFactTable, Double> factWeightMap =
          new HashMap<CubeFactTable, Double>();

      for (CubeFactTable fact : cubeql.getCandidateFactTables()) {
        factWeightMap.put(fact, fact.weight());
      }

      double minWeight = Collections.min(factWeightMap.values());

      for (Iterator<CubeFactTable> i =
          cubeql.getCandidateFactTables().iterator(); i.hasNext();) {
        CubeFactTable fact = i.next();
        if (factWeightMap.get(fact) > minWeight) {
          LOG.info("Removing fact:" + fact +
              " from candidate fact tables as it has more fact weight:"
              + factWeightMap.get(fact) + " minimum:"
              + minWeight);
          i.remove();
        }
      }
    }
  }
}
