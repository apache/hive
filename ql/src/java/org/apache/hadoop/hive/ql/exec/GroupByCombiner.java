/*
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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByCombiner;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.hadoop.hive.ql.exec.Utilities.HAS_REDUCE_WORK;
import static org.apache.hadoop.hive.ql.exec.Utilities.REDUCE_PLAN_NAME;

// Combiner for normal group by operator. In case of map side aggregate, the partially
// aggregated records are sorted based on group by key. If because of some reasons, like hash
// table memory exceeded the limit or the first few batches of records have less ndvs, the
// aggregation is not done, then here the aggregation can be done cheaply as the records
// are sorted based on group by key.
public class GroupByCombiner extends VectorGroupByCombiner {

  private static final Logger LOG = LoggerFactory.getLogger(
          org.apache.hadoop.hive.ql.exec.GroupByCombiner.class.getName());

  private transient GenericUDAFEvaluator[] aggregationEvaluators;
  AbstractSerDe valueSerializer;
  GenericUDAFEvaluator.AggregationBuffer[] aggregationBuffers;
  GroupByOperator groupByOperator;
  ObjectInspector aggrObjectInspector;
  DataInputBuffer valueBuffer;
  Object[] cachedValues;
  DataInputBuffer prevKey;
  BytesWritable valWritable;
  DataInputBuffer prevVal;

  public GroupByCombiner(TaskContext taskContext) throws HiveException, IOException {
    super(taskContext);
    if (rw != null) {
      try {
        groupByOperator = (GroupByOperator) rw.getReducer();

        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        ois.add(keyObjectInspector);
        ois.add(valueObjectInspector);
        ObjectInspector[] rowObjectInspector = new ObjectInspector[1];
        rowObjectInspector[0] =
            ObjectInspectorFactory.getStandardStructObjectInspector(Utilities.reduceFieldNameList,
                        ois);
        groupByOperator.setInputObjInspectors(rowObjectInspector);
        groupByOperator.initializeOp(conf);
        aggregationBuffers = groupByOperator.getAggregationBuffers();
        aggregationEvaluators = groupByOperator.getAggregationEvaluator();

        TableDesc valueTableDesc = rw.getTagToValueDesc().get(0);
        if ((aggregationEvaluators == null) || (aggregationEvaluators.length != numValueCol)) {
          //TODO : Need to support distinct. The logic has to be changed to extract only
          // those aggregates which are not part of distinct.
          LOG.info(" Combiner is disabled as the number of value columns does" +
                  " not match with number of aggregators");
          numValueCol = 0;
          rw = null;
          return;
        }
        valueSerializer = (AbstractSerDe) ReflectionUtils.newInstance(
                valueTableDesc.getSerDeClass(), null);
        valueSerializer.initialize(null,
                valueTableDesc.getProperties(), null);

        aggrObjectInspector = groupByOperator.getAggrObjInspector();
        valueBuffer = new DataInputBuffer();
        cachedValues = new Object[aggregationEvaluators.length];
        prevKey = new DataInputBuffer();
        valWritable = new BytesWritable();
        prevVal = new DataInputBuffer();
      } catch (Exception e) {
        LOG.error(" GroupByCombiner failed", e);
        throw new RuntimeException(e.getMessage());
      }
    }
  }

  private void processAggregation(IFile.Writer writer, DataInputBuffer key)
          throws Exception {
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      cachedValues[i] = aggregationEvaluators[i].evaluate(aggregationBuffers[i]);
    }
    BytesWritable result = (BytesWritable) valueSerializer.serialize(cachedValues,
            aggrObjectInspector);
    valueBuffer.reset(result.getBytes(), result.getLength());
    writer.append(key, valueBuffer);
    combineOutputRecordsCounter.increment(1);
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      aggregationEvaluators[i].reset(aggregationBuffers[i]);
    }
  }

  private void updateAggregation()
          throws HiveException, SerDeException {
    valWritable.set(prevVal.getData(), prevVal.getPosition(),
            prevVal.getLength() - prevVal.getPosition());
    Object row = valueSerializer.deserialize(valWritable);
    groupByOperator.updateAggregation(row);
  }

  private void processRows(TezRawKeyValueIterator rawIter, IFile.Writer writer) {
    long numRows = 0;
    try {
      DataInputBuffer key = rawIter.getKey();
      prevKey.reset(key.getData(), key.getPosition(), key.getLength() - key.getPosition());
      prevVal.reset(rawIter.getValue().getData(), rawIter.getValue().getPosition(),
              rawIter.getValue().getLength() - rawIter.getValue().getPosition());
      int numSameKey = 0;
      while (rawIter.next()) {
        key = rawIter.getKey();
        if (!VectorGroupByCombiner.equals(key, prevKey)) {
          // if current key is not equal to the previous key then we have to emit the
          // record. In case only one record was present for this key, then no need to
          // do aggregation, We can directly append the key and value. For key with more
          // than one record, we have to update the aggregation for the current value only
          // as for previous values (records) aggregation is already done in previous
          // iteration of loop.
          if (numSameKey != 0) {
            updateAggregation();
            processAggregation(writer, prevKey);
          } else {
            writer.append(prevKey, prevVal);
          }
          prevKey.reset(key.getData(), key.getPosition(),
                  key.getLength() - key.getPosition());
          numSameKey = 0;
        } else {
          // If there are more than one record with same key then update the aggregation.
          updateAggregation();
          numSameKey++;
        }
        prevVal.reset(rawIter.getValue().getData(), rawIter.getValue().getPosition(),
                rawIter.getValue().getLength() - rawIter.getValue().getPosition());
        numRows++;
      }
      if (numSameKey != 0) {
        updateAggregation();
        processAggregation(writer, prevKey);
      } else {
        writer.append(prevKey, prevVal);
      }
      combineInputRecordsCounter.increment(numRows);
    } catch(Exception e) {
      LOG.error("processRows failed", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public void combine(TezRawKeyValueIterator rawIter, IFile.Writer writer) {
    try {
      if (!rawIter.next()) {
        return;
      }
      if (numValueCol == 0) {
        // For no aggregation, RLE in writer will take care of reduction.
        appendDirectlyToWriter(rawIter, writer);
      } else {
        processRows(rawIter, writer);
      }
    } catch (IOException e) {
      LOG.error("Failed to combine rows", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  public static JobConf setCombinerInConf(BaseWork dest, JobConf conf, JobConf destConf) {
    if (conf == null || !HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ENABLE_COMBINER_FOR_GROUP_BY)) {
      return conf;
    }

    // As of now, this function is called for all edges. We are interested only on edges
    // to reducer.
    if (!(dest instanceof ReduceWork)) {
      return conf;
    }
    ReduceWork rw = (ReduceWork)dest;
    Operator<?> reducer = rw.getReducer();
    String combinerClass;

    // In case of merge partial only we can have the combiner. In case of reduce side
    // aggregation, the mode will be COMPLETE and reducer expects the records to be raw,
    // not partially aggregated. For example, if its count, then reducer will not do a
    // sum. Instead it will count the number of input records. This will result into
    // invalid result if combiner has aggregated the records already.
    if (reducer instanceof VectorGroupByOperator) {
      VectorGroupByOperator operator = (VectorGroupByOperator) reducer;
      if (operator.getConf().getMode() != GroupByDesc.Mode.MERGEPARTIAL) {
        LOG.info("Combiner not set as operator is not MERGEPARTIAL" + reducer.toString());
        return conf;
      }
      combinerClass = VectorGroupByCombiner.class.getName();
    } else if (reducer instanceof GroupByOperator) {
      GroupByOperator operator = (GroupByOperator) reducer;
      if (operator.getConf().getMode() != GroupByDesc.Mode.MERGEPARTIAL) {
        LOG.info("Combiner not set as operator is not MERGEPARTIAL" + reducer.toString());
        return conf;
      }
      combinerClass = GroupByCombiner.class.getName();
    } else {
      LOG.info("Combiner not set as operator is not GroupByOperator" + reducer.toString());
      return conf;
    }

    conf = new JobConf(conf);
    String plan = HiveConf.getVar(destConf, HiveConf.ConfVars.PLAN);

    // One source vertex can have multiple destination vertex. Add the destination vertex name
    // to the config variable name to distinguish the plan of one reducer from other.
    conf.set(getConfigPrefix(dest.getName()) + HiveConf.ConfVars.PLAN.varname, plan);
    conf.set(getConfigPrefix(dest.getName()) + HAS_REDUCE_WORK, "true");

    // Config with tez.runtime prefix are already part of included config.
    conf.set("tez.runtime.combiner.class", combinerClass);

    // This is adding the reduce work (destination node) plan path in the config for source map node.
    // In the combiner we use the aggregation functions used at the reducer to aggregate
    // the records. The mapper node (plan for map work) will not have these info so we will get
    // this from reduce plan.
    //TODO Need to check if we can get the aggregate info from map plan if its map side
    //aggregate.
    if (destConf.getBoolean(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname, false)) {
      conf.set(getConfigPrefix(dest.getName()) + HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname, "true");
      Path planPath = new Path(plan);
      planPath = new Path(planPath, REDUCE_PLAN_NAME);
      String serializedPlan = destConf.get(planPath.toUri().getPath());
      conf.set(getConfigPrefix(dest.getName()) + planPath.toUri().getPath(), serializedPlan);
    } else {
      conf.set(getConfigPrefix(dest.getName()) + HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname, "false");
    }
    LOG.info("Combiner set for reduce work " + rw.toString() +
            " with reducer " + reducer.toString());
    return conf;
  }
}
