/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.EdgeType;

/**
 * GenTezUtils is a collection of shared helper methods to produce
 * TezWork
 */
public class GenTezUtils {

  static final private Log LOG = LogFactory.getLog(GenTezUtils.class.getName());

  // sequence number is used to name vertices (e.g.: Map 1, Reduce 14, ...)
  private int sequenceNumber = 0;

  // singleton
  private static GenTezUtils utils;

  public static GenTezUtils getUtils() {
    if (utils == null) {
      utils = new GenTezUtils();
    }
    return utils;
  }

  protected GenTezUtils() {
  }

  public void resetSequenceNumber() {
    sequenceNumber = 0;
  }

  public ReduceWork createReduceWork(GenTezProcContext context, Operator<?> root, TezWork tezWork) {
    assert !root.getParentOperators().isEmpty();
    ReduceWork reduceWork = new ReduceWork("Reducer "+ (++sequenceNumber));
    LOG.debug("Adding reduce work (" + reduceWork.getName() + ") for " + root);
    reduceWork.setReducer(root);
    reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

    // All parents should be reduce sinks. We pick the one we just walked
    // to choose the number of reducers. In the join/union case they will
    // all be -1. In sort/order case where it matters there will be only
    // one parent.
    assert context.parentOfRoot instanceof ReduceSinkOperator;
    ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;

    reduceWork.setNumReduceTasks(reduceSink.getConf().getNumReducers());

    setupReduceSink(context, reduceWork, reduceSink);

    tezWork.add(reduceWork);
    tezWork.connect(
        context.preceedingWork,
        reduceWork, EdgeType.SIMPLE_EDGE);

    return reduceWork;
  }

  protected void setupReduceSink(GenTezProcContext context, ReduceWork reduceWork,
      ReduceSinkOperator reduceSink) {

    LOG.debug("Setting up reduce sink: " + reduceSink
        + " with following reduce work: " + reduceWork.getName());

    // need to fill in information about the key and value in the reducer
    GenMapRedUtils.setKeyAndValueDesc(reduceWork, reduceSink);

    // remember which parent belongs to which tag
    reduceWork.getTagToInput().put(reduceSink.getConf().getTag(),
         context.preceedingWork.getName());

    // remember the output name of the reduce sink
    reduceSink.getConf().setOutputName(reduceWork.getName());
  }

  public MapWork createMapWork(GenTezProcContext context, Operator<?> root,
      TezWork tezWork, PrunedPartitionList partitions) throws SemanticException {
    assert root.getParentOperators().isEmpty();
    MapWork mapWork = new MapWork("Map "+ (++sequenceNumber));
    LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    assert root instanceof TableScanOperator;
    String alias = ((TableScanOperator)root).getConf().getAlias();

    setupMapWork(mapWork, context, partitions, root, alias);

    // add new item to the tez work
    tezWork.add(mapWork);

    return mapWork;
  }

  // this method's main use is to help unit testing this class
  protected void setupMapWork(MapWork mapWork, GenTezProcContext context, 
      PrunedPartitionList partitions, Operator<? extends OperatorDesc> root, 
      String alias) throws SemanticException {
    // All the setup is done in GenMapRedUtils
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
        context.inputs, partitions, root, alias, context.conf, false);
  }
}
