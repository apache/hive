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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;
import java.util.List;

public class VectorizedStructColumnReader implements VectorizedColumnReader {

  private final List<VectorizedColumnReader> fieldReaders;

  public VectorizedStructColumnReader(List<VectorizedColumnReader> fieldReaders) {
    this.fieldReaders = fieldReaders;
  }

  @Override
  public void readBatch(
    int total,
    ColumnVector column,
    TypeInfo columnType) throws IOException {
    StructColumnVector structColumnVector = (StructColumnVector) column;
    StructTypeInfo structTypeInfo = (StructTypeInfo) columnType;
    ColumnVector[] vectors = structColumnVector.fields;
    for (int i = 0; i < vectors.length; i++) {
      fieldReaders.get(i)
        .readBatch(total, vectors[i], structTypeInfo.getAllStructFieldTypeInfos().get(i));
      structColumnVector.isRepeating = structColumnVector.isRepeating && vectors[i].isRepeating;

      for (int j = 0; j < vectors[i].isNull.length; j++) {
        structColumnVector.isNull[j] =
          (i == 0) ? vectors[i].isNull[j] : structColumnVector.isNull[j] && vectors[i].isNull[j];
      }
      structColumnVector.noNulls =
        (i == 0) ? vectors[i].noNulls : structColumnVector.noNulls && vectors[i].noNulls;
    }

  }
}
