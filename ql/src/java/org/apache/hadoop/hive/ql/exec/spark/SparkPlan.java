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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SparkPlan {
  private static final String CLASS_NAME = SparkPlan.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(SparkPlan.class);
  private final PerfLogger perfLogger = SessionState.getPerfLogger();

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
    if (LOG.isDebugEnabled()) {
      LOG.info("print generated spark rdd graph:\n" + SparkUtilities.rddGraphToString(finalRDD));
    }
    return finalRDD;
  }

  private void addNumberToTrans() {
    int i = 1;
    String name = null;

    // Traverse leafTran & transGraph add numbers to trans
    for (SparkTran leaf : leafTrans) {
      name = leaf.getName() + " " + i++;
      leaf.setName(name);
    }
    Set<SparkTran> sparkTrans = transGraph.keySet();
    for (SparkTran tran : sparkTrans) {
      name = tran.getName() + " " + i++;
      tran.setName(name);
    }
  }

  private void logSparkPlan() {
    addNumberToTrans();
    ArrayList<SparkTran> leafTran = new ArrayList<SparkTran>();
    leafTran.addAll(leafTrans);

    for (SparkTran leaf : leafTrans) {
      collectLeafTrans(leaf, leafTran);
    }

    // Start Traverse from the leafTrans and get parents of each leafTrans till
    // the end
    StringBuilder sparkPlan = new StringBuilder(
      "\n\t!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Spark Plan !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! \n\n");
    for (SparkTran leaf : leafTran) {
      sparkPlan.append(leaf.getName());
      getSparkPlan(leaf, sparkPlan);
      sparkPlan.append("\n");
    }
    sparkPlan
      .append(" \n\t!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Spark Plan !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ");
    LOG.info(sparkPlan.toString());
  }

  private void collectLeafTrans(SparkTran leaf, List<SparkTran> reduceTrans) {
    List<SparkTran> parents = getParents(leaf);
    if (parents.size() > 0) {
      SparkTran nextLeaf = null;
      for (SparkTran leafTran : parents) {
        if (leafTran instanceof ReduceTran) {
          reduceTrans.add(leafTran);
        } else {
          if (getParents(leafTran).size() > 0)
            nextLeaf = leafTran;
        }
      }
      if (nextLeaf != null)
        collectLeafTrans(nextLeaf, reduceTrans);
    }
  }

  private void getSparkPlan(SparkTran tran, StringBuilder sparkPlan) {
    List<SparkTran> parents = getParents(tran);
    List<SparkTran> nextLeaf = new ArrayList<SparkTran>();
    if (parents.size() > 0) {
      sparkPlan.append(" <-- ");
      boolean isFirst = true;
      for (SparkTran leaf : parents) {
        if (isFirst) {
          sparkPlan.append("( " + leaf.getName());
          if (leaf instanceof ShuffleTran) {
            logShuffleTranStatus((ShuffleTran) leaf, sparkPlan);
          } else {
            logCacheStatus(leaf, sparkPlan);
          }
          isFirst = false;
        } else {
          sparkPlan.append("," + leaf.getName());
          if (leaf instanceof ShuffleTran) {
            logShuffleTranStatus((ShuffleTran) leaf, sparkPlan);
          } else {
            logCacheStatus(leaf, sparkPlan);
          }
        }
        // Leave reduceTran it will be expanded in the next line
        if (getParents(leaf).size() > 0 && !(leaf instanceof ReduceTran)) {
          nextLeaf.add(leaf);
        }
      }
      sparkPlan.append(" ) ");
      if (nextLeaf.size() > 1) {
        logLeafTran(nextLeaf, sparkPlan);
      } else {
        if (nextLeaf.size() != 0)
          getSparkPlan(nextLeaf.get(0), sparkPlan);
      }
    }
  }

  private void logLeafTran(List<SparkTran> parent, StringBuilder sparkPlan) {
    sparkPlan.append(" <-- ");
    boolean isFirst = true;
    for (SparkTran sparkTran : parent) {
      List<SparkTran> parents = getParents(sparkTran);
      SparkTran leaf = parents.get(0);
      if (isFirst) {
        sparkPlan.append("( " + leaf.getName());
        if (leaf instanceof ShuffleTran) {
          logShuffleTranStatus((ShuffleTran) leaf, sparkPlan);
        } else {
          logCacheStatus(leaf, sparkPlan);
        }
        isFirst = false;
      } else {
        sparkPlan.append("," + leaf.getName());
        if (leaf instanceof ShuffleTran) {
          logShuffleTranStatus((ShuffleTran) leaf, sparkPlan);
        } else {
          logCacheStatus(leaf, sparkPlan);
        }
      }
    }
    sparkPlan.append(" ) ");
  }

  private void logShuffleTranStatus(ShuffleTran leaf, StringBuilder sparkPlan) {
    int noOfPartitions = leaf.getNoOfPartitions();
    sparkPlan.append(" ( Partitions " + noOfPartitions);
    SparkShuffler shuffler = leaf.getShuffler();
    sparkPlan.append(", " + shuffler.getName());
    if (leaf.isCacheEnable()) {
      sparkPlan.append(", Cache on");
    } else {
      sparkPlan.append(", Cache off");
    }
  }

  private void logCacheStatus(SparkTran sparkTran, StringBuilder sparkPlan) {
    if (sparkTran.isCacheEnable() != null) {
      if (sparkTran.isCacheEnable().booleanValue()) {
        sparkPlan.append(" (cache on) ");
      } else {
        sparkPlan.append(" (cache off) ");
      }
    }
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
