/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.accumulo.columns;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.hive.accumulo.AccumuloHiveConstants;

import com.google.common.base.Preconditions;

/**
 * {@link ColumnMapping} which corresponds to the Hive column which should be used as the rowID in a
 * {@link Mutation}
 */
public class HiveAccumuloRowIdColumnMapping extends ColumnMapping {

  public HiveAccumuloRowIdColumnMapping(String columnSpec, ColumnEncoding encoding,
      String columnName, String columnType) {
    super(columnSpec, encoding, columnName, columnType);

    // Ensure that we have the correct identifier as the column name
    Preconditions.checkArgument(columnSpec.equalsIgnoreCase(AccumuloHiveConstants.ROWID));
  }

  @Override
  public String toString() {
    return "[" + this.getClass().getSimpleName() + ", " + this.mappingSpec + ", encoding "
        + encoding + "]";
  }
}
