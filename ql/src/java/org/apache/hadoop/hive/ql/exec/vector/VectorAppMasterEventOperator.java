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
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.VectorAppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.VectorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Writable;

import com.google.common.annotations.VisibleForTesting;

/**
 * App Master Event operator implementation.
 **/
public class VectorAppMasterEventOperator extends AppMasterEventOperator
    implements VectorizationOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;
  private VectorAppMasterEventDesc vectorDesc;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean firstBatch;

  private transient VectorExtractRow vectorExtractRow;

  protected transient Object[] singleRow;

  public VectorAppMasterEventOperator(
      CompilationOpContext ctx, OperatorDesc conf, VectorizationContext vContext,
      VectorDesc vectorDesc) {
    super(ctx);
    this.conf = (AppMasterEventDesc) conf;
    this.vContext = vContext;
    this.vectorDesc = (VectorAppMasterEventDesc) vectorDesc;
  }

  /** Kryo ctor. */
  @VisibleForTesting
  public VectorAppMasterEventOperator() {
    super();
  }

  public VectorAppMasterEventOperator(CompilationOpContext ctx) {
    super(ctx);
  }

  @Override
  public void initializeOp(Configuration hconf) throws HiveException {

    // We need a input object inspector that is for the row we will extract out of the
    // vectorized row batch, not for example, an original inspector for an ORC table, etc.
    inputObjInspectors[0] =
        VectorizedBatchUtil.convertToStandardStructObjectInspector((StructObjectInspector) inputObjInspectors[0]);

    // Call AppMasterEventOperator with new input inspector.
    super.initializeOp(hconf);
    firstBatch = true;
  }

  @Override
  public void process(Object data, int tag) throws HiveException {

    if (hasReachedMaxSize) {
      return;
    }

    VectorizedRowBatch batch = (VectorizedRowBatch) data;
    if (firstBatch) {
      vectorExtractRow = new VectorExtractRow();
      vectorExtractRow.init((StructObjectInspector) inputObjInspectors[0], vContext.getProjectedColumns());

      singleRow = new Object[vectorExtractRow.getCount()];

      firstBatch = false;
    }

    ObjectInspector rowInspector = inputObjInspectors[0];
    try {
      Writable writableRow;
      if (batch.selectedInUse) {
        int selected[] = batch.selected;
        for (int logical = 0 ; logical < batch.size; logical++) {
          int batchIndex = selected[logical];
          vectorExtractRow.extractRow(batch, batchIndex, singleRow);
          writableRow = serializer.serialize(singleRow, rowInspector);
          writableRow.write(buffer);
          if (buffer.getLength() > MAX_SIZE) {
            LOG.info("Disabling AM events. Buffer size too large: " + buffer.getLength());
            hasReachedMaxSize = true;
            buffer = null;
            break;
          }
        }
      } else {
        for (int batchIndex = 0 ; batchIndex < batch.size; batchIndex++) {
          vectorExtractRow.extractRow(batch, batchIndex, singleRow);
          writableRow = serializer.serialize(singleRow, rowInspector);
          writableRow.write(buffer);
          if (buffer.getLength() > MAX_SIZE) {
            LOG.info("Disabling AM events. Buffer size too large: " + buffer.getLength());
            hasReachedMaxSize = true;
            buffer = null;
            break;
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }

    forward(data, rowInspector, true);
  }

  @Override
  public VectorizationContext getInputVectorizationContext() {
    return vContext;
  }

  @Override
  public VectorDesc getVectorDesc() {
    return vectorDesc;
  }

}
