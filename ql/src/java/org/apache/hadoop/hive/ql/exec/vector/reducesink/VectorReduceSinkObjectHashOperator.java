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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator.Counter;
import org.apache.hadoop.hive.ql.exec.TerminalOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorExtractRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContextRegion;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesSerialized;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.VectorReduceSinkInfo;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.lazybinary.fast.LazyBinarySerializeWrite;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.base.Preconditions;

/**
 * This class is uniform hash (common) operator class for native vectorized reduce sink.
 */
public class VectorReduceSinkObjectHashOperator extends VectorReduceSinkCommonOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkObjectHashOperator.class.getName();
  private static final Log LOG = LogFactory.getLog(CLASS_NAME);

  protected int[] reduceSinkBucketColumnMap;
  protected TypeInfo[] reduceSinkBucketTypeInfos;

  protected VectorExpression[] reduceSinkBucketExpressions;

  protected int[] reduceSinkPartitionColumnMap;
  protected TypeInfo[] reduceSinkPartitionTypeInfos;

  protected VectorExpression[] reduceSinkPartitionExpressions;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  protected transient Output keyOutput;
  protected transient VectorSerializeRow<BinarySortableSerializeWrite> keyVectorSerializeRow;

  private transient boolean hasBuckets;
  private transient int numBuckets;
  private transient ObjectInspector[] bucketObjectInspectors;
  private transient VectorExtractRow bucketVectorExtractRow;
  private transient Object[] bucketFieldValues;

  private transient boolean isPartitioned;
  private transient ObjectInspector[] partitionObjectInspectors;
  private transient VectorExtractRow partitionVectorExtractRow;
  private transient Object[] partitionFieldValues;
  private transient Random nonPartitionRandom;

  /** Kryo ctor. */
  protected VectorReduceSinkObjectHashOperator() {
    super();
  }

  public VectorReduceSinkObjectHashOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkObjectHashOperator(CompilationOpContext ctx,
      VectorizationContext vContext, OperatorDesc conf) throws HiveException {
    super(ctx, vContext, conf);

    LOG.info("VectorReduceSinkObjectHashOperator constructor vectorReduceSinkInfo " + vectorReduceSinkInfo);

    // This the is Object Hash class variation.
    Preconditions.checkState(!vectorReduceSinkInfo.getUseUniformHash());

    reduceSinkBucketColumnMap = vectorReduceSinkInfo.getReduceSinkBucketColumnMap();
    reduceSinkBucketTypeInfos = vectorReduceSinkInfo.getReduceSinkBucketTypeInfos();
    reduceSinkBucketExpressions = vectorReduceSinkInfo.getReduceSinkBucketExpressions();

    reduceSinkPartitionColumnMap = vectorReduceSinkInfo.getReduceSinkPartitionColumnMap();
    reduceSinkPartitionTypeInfos = vectorReduceSinkInfo.getReduceSinkPartitionTypeInfos();
    reduceSinkPartitionExpressions = vectorReduceSinkInfo.getReduceSinkPartitionExpressions();
  }

  private ObjectInspector[] getObjectInspectorArray(TypeInfo[] typeInfos) {
    final int size = typeInfos.length;
    ObjectInspector[] objectInspectors = new ObjectInspector[size];
    for(int i = 0; i < size; i++) {
      TypeInfo typeInfo = typeInfos[i];
      ObjectInspector standardWritableObjectInspector =
              TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(typeInfo);
      objectInspectors[i] = standardWritableObjectInspector;
    }
    return objectInspectors;
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    keyOutput = new Output();
    keyBinarySortableSerializeWrite.set(keyOutput);
    keyVectorSerializeRow =
        new VectorSerializeRow<BinarySortableSerializeWrite>(
            keyBinarySortableSerializeWrite);
    keyVectorSerializeRow.init(reduceSinkKeyTypeInfos, reduceSinkKeyColumnMap);
 
    hasBuckets = false;
    isPartitioned = false;
    numBuckets = 0;
 
    // Object Hash.

    numBuckets = conf.getNumBuckets();
    hasBuckets = (numBuckets > 0);

    if (hasBuckets) {
      bucketObjectInspectors = getObjectInspectorArray(reduceSinkBucketTypeInfos);
      bucketVectorExtractRow = new VectorExtractRow();
      bucketVectorExtractRow.init(reduceSinkBucketTypeInfos, reduceSinkBucketColumnMap);
      bucketFieldValues = new Object[reduceSinkBucketTypeInfos.length];
    }
  
    isPartitioned = (conf.getPartitionCols() != null);
    if (!isPartitioned) {
      nonPartitionRandom = new Random(12345);
    } else {
      partitionObjectInspectors = getObjectInspectorArray(reduceSinkPartitionTypeInfos);
      LOG.debug("*NEW* partitionObjectInspectors " + Arrays.toString(partitionObjectInspectors));
      partitionVectorExtractRow = new VectorExtractRow();
      partitionVectorExtractRow.init(reduceSinkPartitionTypeInfos, reduceSinkPartitionColumnMap);
      partitionFieldValues = new Object[reduceSinkPartitionTypeInfos.length];
    }
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    try {

      VectorizedRowBatch batch = (VectorizedRowBatch) row;

      batchCounter++;

      if (batch.size == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(CLASS_NAME + " batch #" + batchCounter + " empty");
        }
        return;
      }

      // Perform any key expressions.  Results will go into scratch columns.
      if (reduceSinkKeyExpressions != null) {
        for (VectorExpression ve : reduceSinkKeyExpressions) {
          ve.evaluate(batch);
        }
      }
  
      // Perform any value expressions.  Results will go into scratch columns.
      if (reduceSinkValueExpressions != null) {
        for (VectorExpression ve : reduceSinkValueExpressions) {
          ve.evaluate(batch);
        }
      }
  
      // Perform any bucket expressions.  Results will go into scratch columns.
      if (reduceSinkBucketExpressions != null) {
        for (VectorExpression ve : reduceSinkBucketExpressions) {
          ve.evaluate(batch);
        }
      }
  
      // Perform any partition expressions.  Results will go into scratch columns.
      if (reduceSinkPartitionExpressions != null) {
        for (VectorExpression ve : reduceSinkPartitionExpressions) {
          ve.evaluate(batch);
        }
      }
  
      final boolean selectedInUse = batch.selectedInUse;
      int[] selected = batch.selected;

      final int size = batch.size;
      for (int logical = 0; logical < size; logical++) {
        final int batchIndex = (selectedInUse ? selected[logical] : logical);
  
        final int hashCode;
        if (!hasBuckets) {
          if (!isPartitioned) {
            hashCode = nonPartitionRandom.nextInt();
          } else {
            partitionVectorExtractRow.extractRow(batch, batchIndex, partitionFieldValues);
            hashCode = 
                ObjectInspectorUtils.getBucketHashCode(
                    partitionFieldValues, partitionObjectInspectors);
          }
        } else {
          bucketVectorExtractRow.extractRow(batch, batchIndex, bucketFieldValues);
          final int bucketNum =
              ObjectInspectorUtils.getBucketNumber(
                  bucketFieldValues, bucketObjectInspectors, numBuckets);
          if (!isPartitioned) {
            hashCode = nonPartitionRandom.nextInt() * 31 + bucketNum;
          } else {
            partitionVectorExtractRow.extractRow(batch, batchIndex, partitionFieldValues);
            hashCode = 
                ObjectInspectorUtils.getBucketHashCode(
                    partitionFieldValues, partitionObjectInspectors) * 31 + bucketNum;
          }
        }
  
        keyBinarySortableSerializeWrite.reset();
        keyVectorSerializeRow.serializeWrite(batch, batchIndex);
  
        // One serialized key for 1 or more rows for the duplicate keys.
        final int keyLength = keyOutput.getLength();
        if (tag == -1 || reduceSkipTag) {
          keyWritable.set(keyOutput.getData(), 0, keyLength);
        } else {
          keyWritable.setSize(keyLength + 1);
          System.arraycopy(keyOutput.getData(), 0, keyWritable.get(), 0, keyLength);
          keyWritable.get()[keyLength] = reduceTagByte;
        }
        keyWritable.setDistKeyLength(keyLength);
        keyWritable.setHashCode(hashCode);
  
        valueLazyBinarySerializeWrite.reset();
        valueVectorSerializeRow.serializeWrite(batch, batchIndex);
  
        valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());
  
        collect(keyWritable, valueBytesWritable);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}