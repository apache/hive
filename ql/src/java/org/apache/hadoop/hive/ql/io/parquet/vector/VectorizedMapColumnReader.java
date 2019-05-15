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

package org.apache.hadoop.hive.ql.io.parquet.vector;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * It's column level Parquet reader which is used to read a batch of records for a map column.
 */
public class VectorizedMapColumnReader implements VectorizedColumnReader {
  private VectorizedListColumnReader keyColumnReader;
  private VectorizedListColumnReader valueColumnReader;

  public VectorizedMapColumnReader(VectorizedListColumnReader keyColumnReader,
      VectorizedListColumnReader valueColumnReader) {
    this.keyColumnReader = keyColumnReader;
    this.valueColumnReader = valueColumnReader;
  }

  @Override
  public void readBatch(int total, ColumnVector column, TypeInfo columnType) throws IOException {
    MapColumnVector mapColumnVector = (MapColumnVector) column;
    MapTypeInfo mapTypeInfo = (MapTypeInfo) columnType;
    ListTypeInfo keyListTypeInfo = new ListTypeInfo();
    keyListTypeInfo.setListElementTypeInfo(mapTypeInfo.getMapKeyTypeInfo());
    ListTypeInfo valueListTypeInfo = new ListTypeInfo();
    valueListTypeInfo.setListElementTypeInfo(mapTypeInfo.getMapValueTypeInfo());

    // initialize 2 ListColumnVector for keys and values
    ListColumnVector keyListColumnVector = new ListColumnVector();
    ListColumnVector valueListColumnVector = new ListColumnVector();
    // read the keys and values
    keyColumnReader.readBatch(total, keyListColumnVector, keyListTypeInfo);
    valueColumnReader.readBatch(total, valueListColumnVector, valueListTypeInfo);

    // set the related attributes according to the keys and values
    mapColumnVector.keys = keyListColumnVector.child;
    mapColumnVector.values = valueListColumnVector.child;
    mapColumnVector.isNull = keyListColumnVector.isNull;
    mapColumnVector.offsets = keyListColumnVector.offsets;
    mapColumnVector.lengths = keyListColumnVector.lengths;
    mapColumnVector.childCount = keyListColumnVector.childCount;
    mapColumnVector.isRepeating = keyListColumnVector.isRepeating
        && valueListColumnVector.isRepeating;
  }
}
