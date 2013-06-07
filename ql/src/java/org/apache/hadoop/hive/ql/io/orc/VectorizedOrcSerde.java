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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriter;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpressionWriterFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * A serde class for ORC.
 * It transparently passes the object to/from the ORC file reader/writer.
 */
public class VectorizedOrcSerde extends OrcSerde {
  private final OrcStruct [] orcStructArray = new OrcStruct [VectorizedRowBatch.DEFAULT_SIZE];
  private final Writable [] orcRowArray = new Writable [VectorizedRowBatch.DEFAULT_SIZE];
  private final ObjectWritable ow = new ObjectWritable();
  private final ObjectInspector inspector = null;
  private final VectorExpressionWriter [] valueWriters;

  public VectorizedOrcSerde(ObjectInspector objInspector) {
    super();
    for (int i = 0; i < orcStructArray.length; i++) {
      orcRowArray[i] = new OrcSerdeRow();
    }
    try {
      valueWriters = VectorExpressionWriterFactory
          .getExpressionWriters((StructObjectInspector) objInspector);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }


  @Override
  public Writable serialize(Object obj, ObjectInspector inspector) {
    VectorizedRowBatch batch = (VectorizedRowBatch) obj;
    try {
      for (int i = 0; i < batch.size; i++) {
        OrcStruct ost = orcStructArray[i];
        if (ost == null) {
          ost = new OrcStruct(batch.numCols);
          orcStructArray[i] = ost;
        }
        int index = 0;
        if (batch.selectedInUse) {
          index = batch.selected[i];
        } else {
          index = i;
        }
        for (int p = 0; p < batch.projectionSize; p++) {
          int k = batch.projectedColumns[p];
          Writable w;
          if (batch.cols[k].isRepeating) {
            w = (Writable) valueWriters[p].writeValue(batch.cols[k], 0);
          } else {
            w = (Writable) valueWriters[p].writeValue(batch.cols[k], index);
          }
          ost.setFieldValue(k, w);
        }
        OrcSerdeRow row = (OrcSerdeRow) orcRowArray[i];
        row.realRow = ost;
        row.inspector = inspector;
      }
    } catch (HiveException ex) {
      throw new RuntimeException(ex);
    }
    ow.set(orcRowArray);
    return ow;
  }
}
