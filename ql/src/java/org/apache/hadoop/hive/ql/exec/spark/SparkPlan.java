/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SparkPlan {
  private static final String CLASS_NAME = SparkPlan.class.getName();
  private static final Log LOG = LogFactory.getLog(SparkPlan.class);
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private final Set<SparkTran> rootTrans = new HashSet<SparkTran>();
  private final Set<SparkTran> leafTrans = new HashSet<SparkTran>();
  private final Map<SparkTran, List<SparkTran>> transGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Map<SparkTran, List<SparkTran>> invertedTransGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Set<Integer> cachedRDDIds = new HashSet<Integer>();

  @SuppressWarnings("unchecked")
  public JavaPairRDD<HiveKey, BytesWritable> generateGraph() {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_BUILD_RDD_GRAPH);
    Map<SparkTran, JavaPairRDD<HiveKey, BytesWritable>> tranToOutputRDDMap
        = new HashMap<SparkTran, JavaPairRDD<HiveKey, BytesWritable>>();
    for (SparkTran tran : getAllTrans()) {
      JavaPairRDD<HiveKey, BytesWritable> rdd = null;
      List<SparkTran> parents = getParents(tran);
      if (parents.size() == 0) {
        // Root tran, it must be MapInput
        Preconditions.checkArgument(tran instanceof MapInput,
            "AssertionError: tran must be an instance of MapInput");
        rdd = tran.transform(null);
      } else {
        for (SparkTran parent : parents) {
          JavaPairRDD<HiveKey, BytesWritable> prevRDD = tranToOutputRDDMap.get(parent);
          if (rdd == null) {
            rdd = prevRDD;
          } else {
            rdd = rdd.union(prevRDD);
          }
        }
        rdd = tran.transform(rdd);
      }

      tranToOutputRDDMap.put(tran, rdd);
    }

    logSparkPlan();

    JavaPairRDD<HiveKey, BytesWritable> finalRDD = null;
    for (SparkTran leafTran : leafTrans) {
      JavaPairRDD<HiveKey, BytesWritable> rdd = tranToOutputRDDMap.get(leafTran);
      if (finalRDD == null) {
        finalRDD = rdd;
      } else {
        finalRDD = finalRDD.union(rdd);
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_BUILD_RDD_GRAPH);
    return finalRDD;
  }

  private void logSparkPlan() {
    LOG.info("------------------------------ Spark Plan -----------------------------");
    Set<SparkTran> keySet = invertedTransGraph.keySet();
    for (SparkTran sparkTran : keySet) {
      if (sparkTran instanceof ReduceTran) {
	String sparkPlan = "	" + sparkTran.getName();
	sparkPlan = getSparkPlan(sparkTran, sparkPlan);
	LOG.info(sparkPlan);
      }
    }
    LOG.info("------------------------------ Spark Plan -----------------------------");
  }

  private String getSparkPlan(SparkTran leaf, String sparkPlanMsg) {
    if (leaf != null) {
      List<SparkTran> parents = getParents(leaf);
      if (parents.size() > 0) {
	sparkPlanMsg = sparkPlanMsg + " <-- ";
	boolean isFirst = true;
	SparkTran parent = null;
	for (SparkTran sparkTran : parents) {
	  if (isFirst) {
	    sparkPlanMsg = sparkPlanMsg + "( " + sparkTran.getName();
	    sparkPlanMsg = logCacheStatus(sparkPlanMsg, sparkTran);
	    isFirst = false;
	  } else {
	    sparkPlanMsg = sparkPlanMsg + "," + sparkTran.getName();
	    sparkPlanMsg = logCacheStatus(sparkPlanMsg, sparkTran);
	  }
	  if (getParents(sparkTran).size() > 0 && !(sparkTran instanceof ReduceTran)) {
	    parent = sparkTran;
	  }
	}
	sparkPlanMsg = sparkPlanMsg + " ) ";
	return getSparkPlan(parent, sparkPlanMsg);
      } else {
	return sparkPlanMsg;
      }
    }
    return sparkPlanMsg;
  }

  private String logCacheStatus(String sparkPlanMsg, SparkTran sparkTran) {
    if (sparkTran.isCacheEnable() != null) {
      if (sparkTran.isCacheEnable().booleanValue()) {
	sparkPlanMsg = sparkPlanMsg + " (cache on) ";
      } else {
	sparkPlanMsg = sparkPlanMsg + " (cache off) ";
      }
    }
    return sparkPlanMsg;
  }

  public void addTran(SparkTran tran) {
    rootTrans.add(tran);
    leafTrans.add(tran);
  }

  public void addCachedRDDId(int rddId) {
    cachedRDDIds.add(rddId);
  }

  public Set<Integer> getCachedRDDIds() {
    return cachedRDDIds;
  }

  /**
   * This method returns a topologically sorted list of SparkTran.
   */
  private List<SparkTran> getAllTrans() {
    List<SparkTran> result = new LinkedList<SparkTran>();
    Set<SparkTran> seen = new HashSet<SparkTran>();

    for (SparkTran leaf: leafTrans) {
      // make sure all leaves are visited at least once
      visit(leaf, seen, result);
    }

    return result;
  }

  private void visit(SparkTran child, Set<SparkTran> seen, List<SparkTran> result) {
    if (seen.contains(child)) {
      // don't visit multiple times
      return;
    }

    seen.add(child);

    for (SparkTran parent: getParents(child)) {
      if (!seen.contains(parent)) {
        visit(parent, seen, result);
      }
    }

    result.add(child);
  }

  /**
   * Connects the two SparkTrans in the graph.  Does not allow multiple connections
   * between the same pair of SparkTrans.
   * @param parent
   * @param child
   */
  public void connect(SparkTran parent, SparkTran child) {
    if (getChildren(parent).contains(child)) {
      throw new IllegalStateException("Connection already exists");
    }
    rootTrans.remove(child);
    leafTrans.remove(parent);
    if (transGraph.get(parent) == null) {
      transGraph.put(parent, new LinkedList<SparkTran>());
    }
    if (invertedTransGraph.get(child) == null) {
      invertedTransGraph.put(child, new LinkedList<SparkTran>());
    }
    transGraph.get(parent).add(child);
    invertedTransGraph.get(child).add(parent);
  }

  public List<SparkTran> getParents(SparkTran tran) {
    if (!invertedTransGraph.containsKey(tran)) {
      return new ArrayList<SparkTran>();
    }

    return invertedTransGraph.get(tran);
  }

  public List<SparkTran> getChildren(SparkTran tran) {
    if (!transGraph.containsKey(tran)) {
      return new ArrayList<SparkTran>();
    }

    return transGraph.get(tran);
  }

}
