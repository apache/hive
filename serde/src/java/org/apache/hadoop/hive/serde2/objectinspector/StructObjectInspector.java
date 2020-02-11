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

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;

import java.util.List;

/**
 * StructObjectInspector.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class StructObjectInspector implements ObjectInspector {

  // ** Methods that does not need a data object **
  /**
   * Returns all the fields.
   */
  public abstract List<? extends StructField> getAllStructFieldRefs();

  /**
   * Look up a field.
   */
  public abstract StructField getStructFieldRef(String fieldName);

  // ** Methods that need a data object **
  /**
   * returns null for data = null.
   */
  public abstract Object getStructFieldData(Object data, StructField fieldRef);

  /**
   * returns null for data = null.
   */
  public abstract List<Object> getStructFieldsDataAsList(Object data);

  public boolean isSettable() {
    return false;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    List<? extends StructField> fields = getAllStructFieldRefs();
    sb.append(getClass().getName());
    sb.append("<");
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(fields.get(i).getFieldObjectInspector().toString());
    }
    sb.append(">");
    return sb.toString();
  }
}
