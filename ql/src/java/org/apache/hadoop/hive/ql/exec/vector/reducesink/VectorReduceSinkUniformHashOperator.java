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

package org.apache.hadoop.hive.ql.exec.vector.reducesink;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.keyseries.VectorKeySeriesSerialized;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class is uniform hash (common) operator class for native vectorized reduce sink.
 * There are variation operators for Long, String, and MultiKey.  And, a special case operator
 * for no key (VectorReduceSinkEmptyKeyOperator).
 */
public abstract class VectorReduceSinkUniformHashOperator extends VectorReduceSinkCommonOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkUniformHashOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  // The serialized all null key and its hash code.
  private transient byte[] nullBytes;
  private transient int nullKeyHashCode;

  // The object that determines equal key series.
  protected transient VectorKeySeriesSerialized serializedKeySeries;


  /** Kryo ctor. */
  protected VectorReduceSinkUniformHashOperator() {
    super();
  }

  public VectorReduceSinkUniformHashOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkUniformHashOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    Preconditions.checkState(!isEmptyKey);
    // Create all nulls key.
    try {
      Output nullKeyOutput = new Output();
      keyBinarySortableSerializeWrite.set(nullKeyOutput);
      for (int i = 0; i < reduceSinkKeyColumnMap.length; i++) {
        keyBinarySortableSerializeWrite.writeNull();
      }
      int nullBytesLength = nullKeyOutput.getLength();
      nullBytes = new byte[nullBytesLength];
      System.arraycopy(nullKeyOutput.getData(), 0, nullBytes, 0, nullBytesLength);
      nullKeyHashCode = HashCodeUtil.calculateBytesHashCode(nullBytes, 0, nullBytesLength);
    } catch (Exception e) {
      throw new HiveException(e);
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

      serializedKeySeries.processBatch(batch);

      boolean selectedInUse = batch.selectedInUse;
      int[] selected = batch.selected;

      int logical;
      do {
        if (serializedKeySeries.getCurrentIsAllNull()) {

          // Use the same logic as ReduceSinkOperator.toHiveKey.
          //
          if (tag == -1 || reduceSkipTag) {
            keyWritable.set(nullBytes, 0, nullBytes.length);
          } else {
            keyWritable.setSize(nullBytes.length + 1);
            System.arraycopy(nullBytes, 0, keyWritable.get(), 0, nullBytes.length);
            keyWritable.get()[nullBytes.length] = reduceTagByte;
          }
          keyWritable.setDistKeyLength(nullBytes.length);
          keyWritable.setHashCode(nullKeyHashCode);

        } else {

          // One serialized key for 1 or more rows for the duplicate keys.
          // LOG.info("reduceSkipTag " + reduceSkipTag + " tag " + tag + " reduceTagByte " + (int) reduceTagByte + " keyLength " + serializedKeySeries.getSerializedLength());
          // LOG.info("process offset " + serializedKeySeries.getSerializedStart() + " length " + serializedKeySeries.getSerializedLength());
          final int keyLength = serializedKeySeries.getSerializedLength();
          if (tag == -1 || reduceSkipTag) {
            keyWritable.set(serializedKeySeries.getSerializedBytes(),
                serializedKeySeries.getSerializedStart(), keyLength);
          } else {
            keyWritable.setSize(keyLength + 1);
            System.arraycopy(serializedKeySeries.getSerializedBytes(),
                serializedKeySeries.getSerializedStart(), keyWritable.get(), 0, keyLength);
            keyWritable.get()[keyLength] = reduceTagByte;
          }
          keyWritable.setDistKeyLength(keyLength);
          keyWritable.setHashCode(serializedKeySeries.getCurrentHashCode());
        }

        logical = serializedKeySeries.getCurrentLogical();
        final int end = logical + serializedKeySeries.getCurrentDuplicateCount();
        if (!isEmptyValue) {
          if (selectedInUse) {
            do {
              final int batchIndex = selected[logical];

              valueLazyBinarySerializeWrite.reset();
              valueVectorSerializeRow.serializeWrite(batch, batchIndex);

              valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());

              collect(keyWritable, valueBytesWritable);
            } while (++logical < end);
          } else {
            do {
              valueLazyBinarySerializeWrite.reset();
              valueVectorSerializeRow.serializeWrite(batch, logical);

              valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());

              collect(keyWritable, valueBytesWritable);
            } while (++logical < end);

          }
        } else {

          // Empty value, too.
          do {
            collect(keyWritable, valueBytesWritable);
          } while (++logical < end);
        }

        if (!serializedKeySeries.next()) {
          break;
        }
      } while (true);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}