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

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkPlan {

  private final Set<SparkTran> leafTrans = new HashSet<SparkTran>();
  private final Map<SparkTran, List<SparkTran>> transGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Map<SparkTran, List<SparkTran>> invertedTransGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Map<SparkTran, JavaPairRDD<HiveKey, BytesWritable>> mapInputs =
      new HashMap<SparkTran, JavaPairRDD<HiveKey, BytesWritable>>();

  public void addInput(SparkTran tran, JavaPairRDD<HiveKey, BytesWritable> input) {
    if (!mapInputs.containsKey(tran)) {
      mapInputs.put(tran, input);
      leafTrans.add(tran);
      transGraph.put(tran, new LinkedList<SparkTran>());
      invertedTransGraph.put(tran, new LinkedList<SparkTran>());
    }
  }

  public void execute() throws IllegalStateException {
    Map<SparkTran, JavaPairRDD<HiveKey, BytesWritable>> tranToRDDMap
        = new HashMap<SparkTran, JavaPairRDD<HiveKey, BytesWritable>>();
    for (SparkTran tran : getAllTrans()) {
      JavaPairRDD<HiveKey, BytesWritable> rdd = null;
      if (mapInputs.containsKey(tran)) {
        rdd = mapInputs.get(tran);
      } else {
        // a non-root tran, it must have a previous input
        for (SparkTran parentTran : getParents(tran)) {
          JavaPairRDD<HiveKey, BytesWritable> prevRDD = tranToRDDMap.get(parentTran);
          if (rdd == null) {
            rdd = prevRDD;
          } else {
            rdd = rdd.union(prevRDD);
          }
        }
      }
      rdd = tran.transform(rdd);
      tranToRDDMap.put(tran, rdd);
    }

    JavaPairRDD<HiveKey, BytesWritable> finalRDD = null;
    for (SparkTran leafTran : leafTrans) {
      JavaPairRDD<HiveKey, BytesWritable> rdd = tranToRDDMap.get(leafTran);
      if (finalRDD == null) {
        finalRDD = rdd;
      } else {
        finalRDD = finalRDD.union(rdd);
      }
    }
    finalRDD.foreach(HiveVoidFunction.getInstance());
  }

  /**
   * This method returns a topologically sorted list of SparkTran
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
  public void connect(SparkTran parent, SparkTran child) throws IllegalStateException {
    if (getChildren(parent).contains(child)) {
      throw new IllegalStateException("Connection already exists");
    }
    leafTrans.remove(parent);
    leafTrans.add(child);
    if (transGraph.get(child) == null) {
      transGraph.put(child, new LinkedList<SparkTran>());
    }
    if (invertedTransGraph.get(child) == null) {
      invertedTransGraph.put(child, new LinkedList<SparkTran>());
    }
    transGraph.get(parent).add(child);
    invertedTransGraph.get(child).add(parent);
  }

  public List<SparkTran> getParents(SparkTran tran) throws IllegalStateException {
    if (!invertedTransGraph.containsKey(tran)
        || invertedTransGraph.get(tran) == null) {
      throw new IllegalStateException("Cannot get parent transformations for " + tran);
    }
    return new LinkedList<SparkTran>(invertedTransGraph.get(tran));
  }

  public List<SparkTran> getChildren(SparkTran tran) throws IllegalStateException {
    if (!transGraph.containsKey(tran) || transGraph.get(tran) == null) {
      throw new IllegalStateException("Cannot get children transformations for " + tran);
    }
    return new LinkedList<SparkTran>(transGraph.get(tran));
  }

}
