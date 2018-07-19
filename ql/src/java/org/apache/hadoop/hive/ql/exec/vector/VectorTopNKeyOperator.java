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
package org.apache.hadoop.hive.ql.exec.vector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.ql.plan.VectorTopNKeyDesc;
import org.apache.hadoop.hive.ql.plan.api.OperatorType;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.binarysortable.BinarySortableSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Properties;

import static org.apache.hadoop.hive.ql.plan.api.OperatorType.TOPNKEY;

/**
 * VectorTopNKeyOperator passes rows that contains top N keys only.
 */
public class VectorTopNKeyOperator extends Operator<TopNKeyDesc> implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorTopNKeyDesc vectorDesc;
  private VectorizationContext vContext;

  // Key column info
  private int[] keyColumnNums;
  private TypeInfo[] keyTypeInfos;

  // Extract row
  private transient Object[] singleRow;
  private transient VectorExtractRow vectorExtractRow;

  // Serialization
  private transient BinarySortableSerDe binarySortableSerDe;
  private transient StructObjectInspector keyObjectInspector;

  // Batch processing
  private transient boolean firstBatch;
  private transient PriorityQueue<Writable> priorityQueue;
  private transient int[] temporarySelected;

  public VectorTopNKeyOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) {

    this(ctx);
    this.conf = (TopNKeyDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorTopNKeyDesc) vectorDesc;

    VectorExpression[] keyExpressions = this.vectorDesc.getKeyExpressions();
    final int numKeys = keyExpressions.length;
    keyColumnNums = new int[numKeys];
    keyTypeInfos = new TypeInfo[numKeys];

    for (int i = 0; i < numKeys; i++) {
      keyColumnNums[i] = keyExpressions[i].getOutputColumnNum();
      keyTypeInfos[i] = keyExpressions[i].getOutputTypeInfo();
    }
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorTopNKeyOperator() {
    super();
  }

  public VectorTopNKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    VectorExpression.doTransientInit(vectorDesc.getKeyExpressions());
    for (VectorExpression keyExpression : vectorDesc.getKeyExpressions()) {
      keyExpression.init(hconf);
    }

    this.firstBatch = true;

    VectorExpression[] keyExpressions = vectorDesc.getKeyExpressions();
    final int size = keyExpressions.length;
    ObjectInspector[] fieldObjectInspectors = new ObjectInspector[size];

    for (int i = 0; i < size; i++) {
      VectorExpression keyExpression = keyExpressions[i];
      fieldObjectInspectors[i] = TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
          keyExpression.getOutputTypeInfo());
    }

    keyObjectInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
        this.conf.getKeyColumnNames(), Arrays.asList(fieldObjectInspectors));

    temporarySelected = new int [VectorizedRowBatch.DEFAULT_SIZE];
  }

  @Override
  public void process(Object data, int tag) throws HiveException {
    VectorizedRowBatch batch = (VectorizedRowBatch) data;

    // The selected vector represents selected rows.
    // Clone the selected vector
    System.arraycopy(batch.selected, 0, temporarySelected, 0, batch.size);
    int [] selectedBackup = batch.selected;
    batch.selected = temporarySelected;
    int sizeBackup = batch.size;
    boolean selectedInUseBackup = batch.selectedInUse;

    for (VectorExpression keyExpression : vectorDesc.getKeyExpressions()) {
      keyExpression.evaluate(batch);
    }

    if (firstBatch) {
      vectorExtractRow = new VectorExtractRow();
      vectorExtractRow.init(keyObjectInspector, Ints.asList(keyColumnNums));

      singleRow = new Object[vectorExtractRow.getCount()];
      Comparator comparator = Comparator.reverseOrder();
      priorityQueue = new PriorityQueue<Writable>(comparator);

      try {
        binarySortableSerDe = new BinarySortableSerDe();
        Properties properties = new Properties();
        Joiner joiner = Joiner.on(',');
        properties.setProperty(serdeConstants.LIST_COLUMNS, joiner.join(conf.getKeyColumnNames()));
        properties.setProperty(serdeConstants.LIST_COLUMN_TYPES, joiner.join(keyTypeInfos));
        properties.setProperty(serdeConstants.SERIALIZATION_SORT_ORDER,
            conf.getColumnSortOrder());
        binarySortableSerDe.initialize(getConfiguration(), properties);
      } catch (SerDeException e) {
        throw new HiveException(e);
      }

      firstBatch = false;
    }

    // Clear the priority queue
    priorityQueue.clear();

    // Get top n keys
    for (int i = 0; i < batch.size; i++) {

      // Get keys
      int j;
      if (batch.selectedInUse) {
        j = batch.selected[i];
      } else {
        j = i;
      }
      vectorExtractRow.extractRow(batch, j, singleRow);

      Writable keysWritable;
      try {
        keysWritable = binarySortableSerDe.serialize(singleRow, keyObjectInspector);
      } catch (SerDeException e) {
        throw new HiveException(e);
      }

      // Put the copied keys into the priority queue
      if (!priorityQueue.contains(keysWritable)) {
        priorityQueue.offer(WritableUtils.clone(keysWritable, getConfiguration()));
      }

      // Limit the queue size
      if (priorityQueue.size() > conf.getTopN()) {
        priorityQueue.poll();
      }
    }

    // Filter rows with top n keys
    int size = 0;
    int[] selected = new int[batch.selected.length];
    for (int i = 0; i < batch.size; i++) {
      int j;
      if (batch.selectedInUse) {
        j = batch.selected[i];
      } else {
        j = i;
      }

      // Get keys
      vectorExtractRow.extractRow(batch, j, singleRow);

      Writable keysWritable;
      try {
        keysWritable = binarySortableSerDe.serialize(singleRow, keyObjectInspector);
      } catch (SerDeException e) {
        throw new HiveException(e);
      }

      // Select a row in the priority queue
      if (priorityQueue.contains(keysWritable)) {
        selected[size++] = j;
      }
    }

    // Apply selection to batch
    if (batch.size != size) {
      batch.selectedInUse = true;
      batch.selected = selected;
      batch.size = size;
    }

    // Forward the result
    if (size > 0) {
      forward(batch, null, true);
    }

    // Restore the original selected vector
    batch.selected = selectedBackup;
    batch.size = sizeBackup;
    batch.selectedInUse = selectedInUseBackup;
  }

  @Override
  public String getName() {
    return TopNKeyOperator.getOperatorName();
  }

  @Override
  public OperatorType getType() {
    return TOPNKEY;
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

  // Because a TopNKeyOperator works like a FilterOperator with top n key condition, its properties
  // for optimizers has same values. Following methods are same with FilterOperator;
  // supportSkewJoinOptimization, columnNamesRowResolvedCanBeObtained,
  // supportAutomaticSortMergeJoin, and supportUnionRemoveOptimization.
  @Override
  public boolean supportSkewJoinOptimization() {
    return true;
  }

  @Override
  public boolean columnNamesRowResolvedCanBeObtained() {
    return true;
  }

  @Override
  public boolean supportAutomaticSortMergeJoin() {
    return true;
  }

  @Override
  public boolean supportUnionRemoveOptimization() {
    return true;
  }

  // Must send on to VectorPTFOperator...
  @Override
  public void setNextVectorBatchGroupStatus(boolean isLastGroupBatch) throws HiveException {
    for (Operator<? extends OperatorDesc> op : childOperators) {
      op.setNextVectorBatchGroupStatus(isLastGroupBatch);
    }
  }
}
