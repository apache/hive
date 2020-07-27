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

package org.apache.hive.benchmark.probe;

import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.probe.OrcProbeHashTable;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorSerializeRow;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.binarysortable.fast.BinarySortableSerializeWrite;
import org.apache.hadoop.hive.serde2.fast.SerializeWrite;

import java.io.IOException;

public class OrcDummyProbeMultiKeyHashMultiSet extends OrcProbeHashTable {
  private VectorMapJoinBytesHashMultiSet probeHashMultiHashMultiSet;
  private VectorMapJoinHashMultiSetResult hashMultiHashSetResult;
  // MultiKey vars used for binary sortable conversion
  private SerializeWrite multiKeySerializeWrite;
  private VectorSerializeRow multiKeyVectorSerializeRow;
  private Output currKeyOutput;
  private Output saveKeyOutput;

  public OrcDummyProbeMultiKeyHashMultiSet(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) throws HiveException {
    super(vTable, vInfo);
    this.probeHashMultiHashMultiSet = (VectorMapJoinBytesHashMultiSet) probeDecodeMapJoinTable;
    this.hashMultiHashSetResult =  probeHashMultiHashMultiSet.createHashMultiSetResult();
    this.multiKeySerializeWrite = new BinarySortableSerializeWrite(vInfo.getBigTableKeyColumnMap().length);
    this.multiKeyVectorSerializeRow = new VectorSerializeRow(multiKeySerializeWrite);
    this.currKeyOutput = new Output();
    this.saveKeyOutput = new Output();
  }

  @Override
  public void init() throws HiveException {
    multiKeyVectorSerializeRow.init(probeDecodeMapJoinInfo.getBigTableKeyTypeInfos(), probeDecodeMapJoinInfo.getBigTableKeyColumnMap());
  }

  @Override
  public void filterColumnVector(ColumnVector cv, MutableFilterContext cntx, int batchSize) {
    int[] selected = null;
    int newSize = 0;
    boolean selectedInUse = false;
    ColumnVector probeCol = cv;
    try {
      if (probeCol.isRepeating) {
        // Repeating values case
        multiKeyVectorSerializeRow.setOutput(currKeyOutput);
        multiKeySerializeWrite.reset();
        multiKeyVectorSerializeRow.serializePrimitiveWrite(probeCol, multiKeyVectorSerializeRow.getFields()[0], 0);
        newSize = probeHashMultiHashMultiSet
            .contains(currKeyOutput.getData(), 0, currKeyOutput.getLength(), hashMultiHashSetResult)
            == JoinUtil.JoinResult.MATCH ? batchSize : 0;
      } else {
        Output temp;
        for (int row = 0; row < batchSize; ++row) {
          multiKeyVectorSerializeRow.setOutput(currKeyOutput);
          multiKeySerializeWrite.reset();
          // MultiKey to binary sortable
          multiKeyVectorSerializeRow.serializePrimitiveWrite(probeCol, multiKeyVectorSerializeRow.getFields()[0], row);
          newSize += probeHashMultiHashMultiSet
              .contains(saveKeyOutput.getData(), 0, saveKeyOutput.getLength(), hashMultiHashSetResult)
              == JoinUtil.JoinResult.MATCH ? 1 : 0;
        }
        selectedInUse = true;
      }
      cntx.setFilterContext(selectedInUse, selected, newSize);
      LlapIoImpl.LOG.debug("ProbeDecode Multi Matched: {} selectedInUse {} batchSize {}", newSize, selectedInUse, batchSize);
    } catch (IOException e) {
      LlapIoImpl.LOG.error("ProbeDecode Multi Filter failed: {}", e);
      e.printStackTrace();
    }
  }
}
