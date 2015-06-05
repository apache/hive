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

import java.util.Collection;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

/**
 * This is the *NON-NATIVE* vector map join operator for just LEFT OUTER JOIN and filtered.
 *
 * It is a row pass-thru so that super MapJoinOperator can do the outer join filtering properly.
 *
 */
public class VectorMapJoinOuterFilteredOperator extends VectorMapJoinBaseOperator {

  private static final long serialVersionUID = 1L;

  private VectorizationContext vContext;

  // The above members are initialized by the constructor and must not be
  // transient.
  //---------------------------------------------------------------------------

  private transient boolean firstBatch;

  private transient VectorExtractRowDynBatch vectorExtractRowDynBatch;

  protected transient Object[] singleRow;

  public VectorMapJoinOuterFilteredOperator() {
    super();
  }

  public VectorMapJoinOuterFilteredOperator(VectorizationContext vContext, OperatorDesc conf)
      throws HiveException {
    super(vContext, conf);

    this.vContext = vContext;
  }

  @Override
  public Collection<Future<?>> initializeOp(Configuration hconf) throws HiveException {

    final int posBigTable = conf.getPosBigTable();

    // We need a input object inspector that is for the row we will extract out of the
    // vectorized row batch, not for example, an original inspector for an ORC table, etc.
    inputObjInspectors[posBigTable] =
        VectorizedBatchUtil.convertToStandardStructObjectInspector((StructObjectInspector) inputObjInspectors[posBigTable]);

    // Call super VectorMapJoinOuterFilteredOperator, which calls super MapJoinOperator with
    // new input inspector.
    Collection<Future<?>> result = super.initializeOp(hconf);

    firstBatch = true;

    return result;
  }

  @Override
  public void process(Object data, int tag) throws HiveException {

    VectorizedRowBatch batch = (VectorizedRowBatch) data;

    // Preparation for hybrid grace hash join
    this.tag = tag;
    if (scratchBatch == null) {
      scratchBatch = VectorizedBatchUtil.makeLike(batch);
    }

    if (firstBatch) {
      vectorExtractRowDynBatch = new VectorExtractRowDynBatch();
      vectorExtractRowDynBatch.init((StructObjectInspector) inputObjInspectors[0], vContext.getProjectedColumns());

      singleRow = new Object[vectorExtractRowDynBatch.getCount()];

      firstBatch = false;
    }


    vectorExtractRowDynBatch.setBatchOnEntry(batch);

    // VectorizedBatchUtil.debugDisplayBatch( batch, "VectorReduceSinkOperator processOp ");

    if (batch.selectedInUse) {
      int selected[] = batch.selected;
      for (int logical = 0 ; logical < batch.size; logical++) {
        int batchIndex = selected[logical];
        vectorExtractRowDynBatch.extractRow(batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    } else {
      for (int batchIndex = 0 ; batchIndex < batch.size; batchIndex++) {
        vectorExtractRowDynBatch.extractRow(batchIndex, singleRow);
        super.process(singleRow, tag);
      }
    }

    vectorExtractRowDynBatch.forgetBatchOnExit();
  }
}
