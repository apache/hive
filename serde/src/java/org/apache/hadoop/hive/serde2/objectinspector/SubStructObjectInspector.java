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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.List;

/**
 * SubsetStructObjectInspector implement a wrapper around a base object inspector (baseOI)
 * such that when the row corresponding to the baseOI is given together with this object
 * inspector, it will mask out some fields in the row as if they are not there.
 */
public class SubStructObjectInspector extends StructObjectInspector {

  protected StructObjectInspector baseOI; // base object inspector
  protected int startCol; // start column number
  protected int numCols;  // number of columns
  protected List<StructField> fields;

  /**
   * Create a new Object Inspector based on a base object inspector and the subset of
   * columns will be inspected (from startCol to startCol+numCols).
   * @param baseOI
   * @param startCol
   * @param numCols
   */
  public SubStructObjectInspector(StructObjectInspector baseOI, int startCol, int numCols) {

    this.baseOI = baseOI;
    this.startCol = startCol;
    this.numCols = numCols;

    List<? extends StructField> baseFields = baseOI.getAllStructFieldRefs();
    assert startCol < baseFields.size() && startCol + numCols < baseFields.size();
    this.fields = new ArrayList<StructField>(numCols);
    this.fields.addAll(baseOI.getAllStructFieldRefs().subList(startCol, startCol + numCols));
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return this.fields;
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    return baseOI.getStructFieldData(data, fieldRef);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    return baseOI.getStructFieldsDataAsList(data).subList(startCol, startCol+numCols);
  }
}
