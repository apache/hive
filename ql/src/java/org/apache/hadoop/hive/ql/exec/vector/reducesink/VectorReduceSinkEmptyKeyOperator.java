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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class is the UniformHash empty key operator class for native vectorized reduce sink.
 *
 * Since there is no key, we initialize the keyWritable once with an empty value.
 */
public class VectorReduceSinkEmptyKeyOperator extends VectorReduceSinkCommonOperator {

  private static final long serialVersionUID = 1L;
  private static final String CLASS_NAME = VectorReduceSinkEmptyKeyOperator.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean isKeyInitialized;

  /** Kryo ctor. */
  protected VectorReduceSinkEmptyKeyOperator() {
    super();
  }

  public VectorReduceSinkEmptyKeyOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  public VectorReduceSinkEmptyKeyOperator(CompilationOpContext ctx, OperatorDesc conf,
      VectorizationContext vContext, VectorDesc vectorDesc) throws HiveException {
    super(ctx, conf, vContext, vectorDesc);

    LOG.info("VectorReduceSinkEmptyKeyOperator constructor vectorReduceSinkInfo " + vectorReduceSinkInfo);

  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);

    isKeyInitialized = false;

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

      if (!isKeyInitialized) {
        isKeyInitialized = true;
        Preconditions.checkState(isEmptyKey);
        initializeEmptyKey(tag);
      }

      // Perform any value expressions.  Results will go into scratch columns.
      if (reduceSinkValueExpressions != null) {
        for (VectorExpression ve : reduceSinkValueExpressions) {
          ve.evaluate(batch);
        }
      }

      final int size = batch.size;
      if (!isEmptyValue) {
        if (batch.selectedInUse) {
          int[] selected = batch.selected;
          for (int logical = 0; logical < size; logical++) {
            final int batchIndex = selected[logical];

            valueLazyBinarySerializeWrite.reset();
            valueVectorSerializeRow.serializeWrite(batch, batchIndex);

            valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());

            collect(keyWritable, valueBytesWritable);
          }
        } else {
          for (int batchIndex = 0; batchIndex < size; batchIndex++) {
            valueLazyBinarySerializeWrite.reset();
            valueVectorSerializeRow.serializeWrite(batch, batchIndex);

            valueBytesWritable.set(valueOutput.getData(), 0, valueOutput.getLength());

            collect(keyWritable, valueBytesWritable);
          }
        }
      } else {

        // Empty value, too.
        for (int i = 0; i < size; i++) {
          collect(keyWritable, valueBytesWritable);
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}