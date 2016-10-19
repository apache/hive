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

import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOrderedMap.Mapping;

/**
 * This class collects column information for copying a row from one VectorizedRowBatch to
 * same/another batch.
 *
 * In this variation, column information is ordered by the source column number.
 */
public class VectorColumnSourceMapping extends VectorColumnMapping {

  private static final long serialVersionUID = 1L;

  public VectorColumnSourceMapping(String name) {
    super(name);
  }

  @Override
  public void add(int sourceColumn, int outputColumn, String typeName) {
    // Order on sourceColumn.
    vectorColumnMapping.add(sourceColumn, outputColumn, typeName);
  }

  @Override
  public void finalize() {
    Mapping mapping = vectorColumnMapping.getMapping();

    // Ordered columns are the source columns.
    sourceColumns = mapping.getOrderedColumns();
    outputColumns = mapping.getValueColumns();
    typeNames = mapping.getTypeNames();

    // Not needed anymore.
    vectorColumnMapping = null;
  }

  public boolean isSourceSequenceGood() {
   int count = sourceColumns.length;
   for (int i = 0; i < count; i++) {
      if (sourceColumns[i] != i) {
        return false;
      }
   }
   return true;
  }
}