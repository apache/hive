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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinaryDeserializeRead;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.mapreduce.combine.MRCombiner;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.api.impl.TezInputContextImpl;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.library.common.sort.impl.IFile;
import org.apache.tez.runtime.library.common.sort.impl.TezRawKeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import static org.apache.hadoop.hive.ql.exec.Utilities.HAS_REDUCE_WORK;
import static org.apache.hadoop.hive.ql.exec.Utilities.MAPRED_REDUCER_CLASS;
import static org.apache.hadoop.hive.ql.exec.Utilities.REDUCE_PLAN_NAME;
import static org.apache.hadoop.hive.serde2.lazy.fast.LazySimpleDeserializeRead.byteArrayCompareRanges;

// Combiner for vectorized group by operator. In case of map side aggregate, the partially
// aggregated records are sorted based on group by key. If because of some reasons, like hash
// table memory exceeded the limit or the first few batches of records have less ndvs, the
// aggregation is not done, then here the aggregation can be done cheaply as the records
// are sorted based on group by key.
public class VectorGroupByCombiner extends MRCombiner {
  private static final Logger LOG = LoggerFactory.getLogger(
      VectorGroupByCombiner.class.getName());
  protected final Configuration conf;
  protected final TezCounter combineInputRecordsCounter;
  protected final TezCounter combineOutputRecordsCounter;
  VectorAggregateExpression[] aggregators;
  VectorAggregationBufferRow aggregationBufferRow;
  protected transient LazyBinarySerializeWrite valueLazyBinarySerializeWrite;

  // This helper object serializes LazyBinary format reducer values from columns of a row
  // in a vectorized row batch.
  protected transient VectorSerializeRow<LazyBinarySerializeWrite> valueVectorSerializeRow;

  // The output buffer used to serialize a value into.
  protected transient ByteStream.Output valueOutput;
  DataInputBuffer valueBytesWritable;

  // Only required minimal configs are copied to the worker nodes. This hack (file.) is
  // done to include these configs to be copied to the worker node.
  protected static String confPrefixForWorker = "file.";

  VectorDeserializeRow<LazyBinaryDeserializeRead> batchValueDeserializer;
  int firstValueColumnOffset;
  VectorizedRowBatchCtx batchContext = null;
  protected int numValueCol = 0;
  protected ReduceWork rw;
  VectorizedRowBatch outputBatch = null;
  VectorizedRowBatch inputBatch = null;
  protected Deserializer inputKeyDeserializer = null;
  protected ObjectInspector keyObjectInspector = null;
  protected ObjectInspector valueObjectInspector = null;
  protected StructObjectInspector valueStructInspectors = null;
  protected StructObjectInspector keyStructInspector = null;

  public VectorGroupByCombiner(TaskContext taskContext) throws HiveException, IOException {
    super(taskContext);

    combineInputRecordsCounter =
            taskContext.getCounters().findCounter(TaskCounter.COMBINE_INPUT_RECORDS);
    combineOutputRecordsCounter =
            taskContext.getCounters().findCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);

    conf = TezUtils.createConfFromUserPayload(taskContext.getUserPayload());
    rw = getReduceWork(taskContext);
    if (rw == null) {
      return;
    }

    if (rw.getReducer() instanceof VectorGroupByOperator) {
      VectorGroupByOperator vectorGroupByOperator = (VectorGroupByOperator) rw.getReducer();
      vectorGroupByOperator.initializeOp(this.conf);
      this.aggregators = vectorGroupByOperator.getAggregators();
      this.aggregationBufferRow = allocateAggregationBuffer();
      batchContext = rw.getVectorizedRowBatchCtx();
      if ((aggregators == null) || (aggregators.length != numValueCol)) {
        //TODO : Need to support distinct. The logic has to be changed to extract only
        // those aggregates which are not part of distinct.
        LOG.info(" Combiner is disabled as the number of value columns does" +
                " not match with number of aggregators");
        rw = null;
        numValueCol = 0;
        return;
      }
    }

    try {
      initObjectInspectors(rw.getTagToValueDesc().get(0), rw.getKeyDesc());
      if (batchContext != null && numValueCol > 0) {
        initVectorBatches();
      }
    } catch (SerDeException e) {
      LOG.error("Fail to initialize VectorGroupByCombiner.", e);
      throw new RuntimeException(e.getCause());
    }
  }

  protected static String getConfigPrefix(String destName) {
    return confPrefixForWorker + destName;
  }

  // Get the reduce work from the config. Here some hack is used to prefix the config name with
  // "file." to avoid the config being filtered out.
  private ReduceWork getReduceWork(TaskContext context) {
    String destVertexName;
    if (context instanceof TezOutputContextImpl) {
      destVertexName = ((TezOutputContextImpl)context).getDestinationVertexName();
    } else {
      // As of now only map side combiner is supported.
      return null;
    }

    String plan =  conf.get(getConfigPrefix(destVertexName) +
            HiveConf.ConfVars.PLAN.varname);
    if (plan == null) {
      LOG.info("Reduce plan is not set for vertex " + destVertexName);
      return null;
    }
    this.conf.set(HiveConf.ConfVars.PLAN.varname, plan);
    if (conf.getBoolean(getConfigPrefix(destVertexName)
                    + HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname,
            true)) {
      Path planPath = new Path(plan);
      planPath = new Path(planPath, REDUCE_PLAN_NAME);
      String planString = conf.get(getConfigPrefix(destVertexName) +
              planPath.toUri().getPath());
      this.conf.set(planPath.toUri().getPath(), planString);
      this.conf.set(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname, "true");
    } else {
      this.conf.set(HiveConf.ConfVars.HIVE_RPC_QUERY_PLAN.varname, "false");
    }
    this.conf.set(HAS_REDUCE_WORK, "true");
    this.conf.set(MAPRED_REDUCER_CLASS, ExecReducer.class.getName());

    return Utilities.getReduceWork(conf);
  }

  private void initObjectInspectors(TableDesc valueTableDesc,TableDesc keyTableDesc)
          throws SerDeException {
    inputKeyDeserializer =
            ReflectionUtils.newInstance(keyTableDesc.getDeserializerClass(), null);
    SerDeUtils.initializeSerDe(inputKeyDeserializer, null,
            keyTableDesc.getProperties(), null);
    keyObjectInspector = inputKeyDeserializer.getObjectInspector();

    keyStructInspector = (StructObjectInspector) keyObjectInspector;
    firstValueColumnOffset = keyStructInspector.getAllStructFieldRefs().size();

    Deserializer inputValueDeserializer = (AbstractSerDe) ReflectionUtils.newInstance(
            valueTableDesc.getDeserializerClass(), null);
    SerDeUtils.initializeSerDe(inputValueDeserializer, null,
            valueTableDesc.getProperties(), null);
    valueObjectInspector = inputValueDeserializer.getObjectInspector();
    valueStructInspectors = (StructObjectInspector) valueObjectInspector;
    numValueCol = valueStructInspectors.getAllStructFieldRefs().size();
  }

  void initVectorBatches() throws HiveException {
    inputBatch = batchContext.createVectorizedRowBatch();

    // Create data buffers for value bytes column vectors.
    for (int i = firstValueColumnOffset; i < inputBatch.numCols; i++) {
      ColumnVector colVector = inputBatch.cols[i];
      if (colVector instanceof BytesColumnVector) {
        BytesColumnVector bytesColumnVector = (BytesColumnVector) colVector;
        bytesColumnVector.initBuffer();
      }
    }

    batchValueDeserializer =
            new VectorDeserializeRow<>(
                    new LazyBinaryDeserializeRead(
                            VectorizedBatchUtil.typeInfosFromStructObjectInspector(
                                    valueStructInspectors),
                            true));
    batchValueDeserializer.init(firstValueColumnOffset);

    int[] valueColumnMap = new int[numValueCol];
    for (int i = 0; i < numValueCol; i++) {
      valueColumnMap[i] = i + firstValueColumnOffset;
    }

    valueLazyBinarySerializeWrite = new LazyBinarySerializeWrite(numValueCol);
    valueVectorSerializeRow = new VectorSerializeRow<>(valueLazyBinarySerializeWrite);
    valueVectorSerializeRow.init(VectorizedBatchUtil.typeInfosFromStructObjectInspector(
            valueStructInspectors), valueColumnMap);
    valueOutput = new ByteStream.Output();
    valueVectorSerializeRow.setOutput(valueOutput);
    outputBatch = batchContext.createVectorizedRowBatch();
    valueBytesWritable = new DataInputBuffer();
  }

  private VectorAggregationBufferRow allocateAggregationBuffer() throws HiveException {
    VectorAggregateExpression.AggregationBuffer[] aggregationBuffers =
            new VectorAggregateExpression.AggregationBuffer[aggregators.length];
    for (int i=0; i < aggregators.length; ++i) {
      aggregationBuffers[i] = aggregators[i].getNewAggregationBuffer();
      aggregators[i].reset(aggregationBuffers[i]);
    }
    return new VectorAggregationBufferRow(aggregationBuffers);
  }

  private void finishAggregation(DataInputBuffer key, IFile.Writer writer, boolean needFlush)
          throws HiveException, IOException {
    for (int i = 0; i < aggregators.length; ++i) {
      try {
        aggregators[i].aggregateInput(aggregationBufferRow.getAggregationBuffer(i), inputBatch);
      } catch (HiveException e) {
        throw new RuntimeException(e.getCause());
      }
    }

    // In case the input batch is full but the keys are still same we need not flush.
    // Only evaluate the aggregates and store it in the aggregationBufferRow. The aggregate
    // functions are incremental and will take care of correctness when next batch comes for
    // aggregation.
    if (!needFlush) {
      return;
    }

    int colNum = firstValueColumnOffset;
    for (int i = 0; i < aggregators.length; ++i) {
      aggregators[i].assignRowColumn(outputBatch, 0, colNum++,
              aggregationBufferRow.getAggregationBuffer(i));
    }

    valueLazyBinarySerializeWrite.reset();
    valueVectorSerializeRow.serializeWrite(outputBatch, 0);
    valueBytesWritable.reset(valueOutput.getData(), 0, valueOutput.getLength());
    writer.append(key, valueBytesWritable);
    combineOutputRecordsCounter.increment(1);
    aggregationBufferRow.reset();
    outputBatch.reset();
  }

  private void addValueToBatch(DataInputBuffer val, DataInputBuffer key,
                      IFile.Writer writer, boolean needFLush) throws IOException, HiveException {
    batchValueDeserializer.setBytes(val.getData(), val.getPosition(),
            val.getLength() - val.getPosition());
    batchValueDeserializer.deserialize(inputBatch, inputBatch.size);
    inputBatch.size++;
    if (needFLush || (inputBatch.size >= VectorizedRowBatch.DEFAULT_SIZE)) {
      processVectorGroup(key, writer, needFLush);
    }
  }

  private void processVectorGroup(DataInputBuffer key, IFile.Writer writer, boolean needFlush)
          throws HiveException {
    try {
      finishAggregation(key, writer, needFlush);
      inputBatch.reset();
    } catch (Exception e) {
      String rowString;
      try {
        rowString = inputBatch.toString();
      } catch (Exception e2) {
        rowString = "[Error getting row data with exception "
                + StringUtils.stringifyException(e2) + " ]";
      }
      LOG.error("Hive Runtime Error while processing vector batch" + rowString, e);
      throw new HiveException("Hive Runtime Error while processing vector batch", e);
    }
  }

  protected void appendDirectlyToWriter(TezRawKeyValueIterator rawIter, IFile.Writer writer) {
    long numRows = 0;
    try {
      do {
        numRows++;
        writer.append(rawIter.getKey(), rawIter.getValue());
      } while (rawIter.next());
      combineInputRecordsCounter.increment(numRows);
      combineOutputRecordsCounter.increment(numRows);
    } catch(IOException e) {
      LOG.error("Append to writer failed", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  private void appendToWriter(DataInputBuffer val, DataInputBuffer key, IFile.Writer writer) {
    try {
      writer.append(key, val);
      combineOutputRecordsCounter.increment(1);
    } catch(IOException e) {
      LOG.error("Append value list to writer failed", e);
      throw new RuntimeException(e.getMessage());
    }
  }

  public static boolean compare(DataInputBuffer buf1, DataInputBuffer buf2) {
    byte[] b1 = buf1.getData();
    int s1 = buf1.getPosition();
    int l1 = buf1.getLength() - buf1.getPosition();
    byte[] b2 = buf2.getData();
    int s2 = buf2.getPosition();
    int l2 = buf2.getLength() - buf2.getPosition();
    return org.apache.hadoop.hive.ql.exec.util.FastByteComparisons.equal(b1, s1, l1, b2, s2, l2);
  }

  @Override
  public void combine(TezRawKeyValueIterator rawIter, IFile.Writer writer) {
    try {
      if (!rawIter.next()) {
        return;
      }

      if (numValueCol == 0) {
        // For no aggregation, RLE in writer will take care of reduction.
        //TODO this can be optimized further as RLE will still write length of value.
        appendDirectlyToWriter(rawIter, writer);
        return;
      }

      DataInputBuffer key = rawIter.getKey();
      DataInputBuffer prevKey = new DataInputBuffer();
      prevKey.reset(key.getData(), key.getPosition(), key.getLength() - key.getPosition());
      DataInputBuffer valTemp = rawIter.getValue();
      DataInputBuffer val = new DataInputBuffer();
      val.reset(valTemp.getData(), valTemp.getPosition(),
              valTemp.getLength() - valTemp.getPosition());

      int numValues = 1;
      long numRows = 1;

      while (rawIter.next()) {
        key = rawIter.getKey();
        if (!compare(prevKey, key)) {
          if (numValues == 1) {
            // if key has single record, no need for aggregation.
            appendToWriter(val, prevKey, writer);
          } else {
            addValueToBatch(val, prevKey, writer, true);
          }
          prevKey.reset(key.getData(), key.getPosition(),
                  key.getLength() - key.getPosition());
          numValues = 0;
        } else {
          addValueToBatch(val, prevKey, writer, false);
        }
        valTemp = rawIter.getValue();
        val.reset(valTemp.getData(), valTemp.getPosition(),
                valTemp.getLength() - valTemp.getPosition());
        numRows++;
        numValues++;
      }

      // Process the last key.
      if (numValues == 1) {
        appendToWriter(val, prevKey, writer);
      } else {
        addValueToBatch(val, prevKey, writer, true);
      }
      combineInputRecordsCounter.increment(numRows);
    } catch (IOException | HiveException e) {
      LOG.error("Failed to combine rows", e);
      throw new RuntimeException(e.getMessage());
    }
  }
}
