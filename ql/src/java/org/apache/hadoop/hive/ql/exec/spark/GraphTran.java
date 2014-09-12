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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;

public class GraphTran {

  private final Set<SparkTran> rootTrans = new HashSet<SparkTran>();
  private final Set<SparkTran> leafTrans = new HashSet<SparkTran>();
  private final Map<SparkTran, List<SparkTran>> transGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Map<SparkTran, List<SparkTran>> invertedTransGraph = new HashMap<SparkTran, List<SparkTran>>();
  private final Map<SparkTran, List<JavaPairRDD<HiveKey, BytesWritable>>> unionInputs =
      new HashMap<SparkTran, List<JavaPairRDD<HiveKey, BytesWritable>>>();
  private final Map<SparkTran, JavaPairRDD<BytesWritable, BytesWritable>> mapInputs = new HashMap<SparkTran, JavaPairRDD<BytesWritable, BytesWritable>>();

  public void addRootTranWithInput(SparkTran tran, JavaPairRDD<BytesWritable, BytesWritable> input) {
    if (!rootTrans.contains(tran)) {
      rootTrans.add(tran);
      leafTrans.add(tran);
      transGraph.put(tran, new LinkedList<SparkTran>());
      invertedTransGraph.put(tran, new LinkedList<SparkTran>());
    }
    if (input != null) {
      mapInputs.put(tran, input);
    }
  }

  public void execute() throws IllegalStateException {
    Map<SparkTran, JavaPairRDD<HiveKey, BytesWritable>> resultRDDs =
        new HashMap<SparkTran, JavaPairRDD<HiveKey, BytesWritable>>();
    for (SparkTran tran : rootTrans) {
      // make sure all the root trans are MapTran
      if (!(tran instanceof MapTran)) {
        throw new IllegalStateException("root transformations must be MapTran!");
      }
      JavaPairRDD<BytesWritable, BytesWritable> input = mapInputs.get(tran);
      if (input == null) {
        throw new IllegalStateException("input is missing for transformation!");
      }
      JavaPairRDD<HiveKey, BytesWritable> rdd = tran.transform(input);

      while (getChildren(tran).size() > 0) {
        SparkTran childTran = getChildren(tran).get(0);
        if (childTran instanceof UnionTran) {
          List<JavaPairRDD<HiveKey, BytesWritable>> unionInputList = unionInputs
              .get(childTran);
          if (unionInputList == null) {
            // process the first union input RDD, cache it in the hash map
            unionInputList = new LinkedList<JavaPairRDD<HiveKey, BytesWritable>>();
            unionInputList.add(rdd);
            unionInputs.put(childTran, unionInputList);
            break;
          } else if (unionInputList.size() < this.getParents(childTran).size() - 1) {
            // not the last input RDD yet, continue caching it in the hash map
            unionInputList.add(rdd);
            break;
          } else if (unionInputList.size() == this.getParents(childTran).size() - 1) { // process
            // process the last input RDD
            for (JavaPairRDD<HiveKey, BytesWritable> inputRDD : unionInputList) {
              ((UnionTran) childTran).setOtherInput(inputRDD);
              rdd = childTran.transform(rdd);
            }
          }
        } else {
          rdd = childTran.transform(rdd);
        }
        tran = childTran;
      }
      // if the current transformation is a leaf tran and it has not got processed yet, cache its corresponding RDD 
      if (!resultRDDs.containsKey(tran) && getChildren(tran).isEmpty()) {
        resultRDDs.put(tran, rdd);
      }
    }
    for (JavaPairRDD<HiveKey, BytesWritable> resultRDD : resultRDDs.values()) {
      resultRDD.foreach(HiveVoidFunction.getInstance());
    }
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
