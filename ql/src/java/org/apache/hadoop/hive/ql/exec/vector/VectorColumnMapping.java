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

import java.util.Arrays;

/**
 * This class collects column information for copying a row from one VectorizedRowBatch to
 * same/another batch.
 */
public abstract class VectorColumnMapping {

  private static final long serialVersionUID = 1L;

  protected int[] sourceColumns;
  protected int[] outputColumns;
  protected String[] typeNames;

  protected VectorColumnOrderedMap vectorColumnMapping;

  public VectorColumnMapping(String name) {
    this.vectorColumnMapping = new VectorColumnOrderedMap(name);
  }

  public abstract void add(int sourceColumn, int outputColumn, String typeName);

  public abstract void finalize();

  public int getCount() {
    return sourceColumns.length;
  }

  public int[] getInputColumns() {
    return sourceColumns;
  }

  public int[] getOutputColumns() {
    return outputColumns;
  }

  public String[] getTypeNames() {
    return typeNames;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("source columns: " + Arrays.toString(sourceColumns));
    sb.append(", ");
    sb.append("output columns: " + Arrays.toString(outputColumns));
    sb.append(", ");
    sb.append("type names: " + Arrays.toString(typeNames));
    return sb.toString();
  }
}
