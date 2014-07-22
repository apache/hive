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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.data.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hive.hcatalog.common.HCatException;

/**
 * HCatSchema. This class is NOT thread-safe.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatSchema implements Serializable {

  private static final long serialVersionUID = 1L;

  private final List<HCatFieldSchema> fieldSchemas;
  //HCatFieldSchema.getName()->position
  private final Map<String, Integer> fieldPositionMap;
  private final List<String> fieldNames;

  /**
   *
   * @param fieldSchemas is now owned by HCatSchema. Any subsequent modifications
   * on fieldSchemas won't get reflected in HCatSchema.  Each fieldSchema's name
   * in the list must be unique, otherwise throws IllegalArgumentException.
   */
  public HCatSchema(final List<HCatFieldSchema> fieldSchemas) {
    this.fieldSchemas = new ArrayList<HCatFieldSchema>(fieldSchemas);
    int idx = 0;
    fieldPositionMap = new HashMap<String, Integer>();
    fieldNames = new ArrayList<String>();
    for (HCatFieldSchema field : fieldSchemas) {
      if (field == null)
        throw new IllegalArgumentException("Field cannot be null");

      String fieldName = normalizeName(field.getName());
      if (fieldPositionMap.containsKey(fieldName))
        throw new IllegalArgumentException("Field named " + field.getName() +
          " already exists");
      fieldPositionMap.put(fieldName, idx);
      fieldNames.add(fieldName);
      idx++;
    }
  }

  public void append(final HCatFieldSchema hfs) throws HCatException {
    if (hfs == null)
      throw new HCatException("Attempt to append null HCatFieldSchema in HCatSchema.");

    String fieldName = normalizeName(hfs.getName());
    if (fieldPositionMap.containsKey(fieldName))
      throw new HCatException("Attempt to append HCatFieldSchema with already " +
        "existing name: " + fieldName + ".");

    this.fieldSchemas.add(hfs);
    this.fieldNames.add(fieldName);
    this.fieldPositionMap.put(fieldName, this.size() - 1);
  }

  /**
   *  Users are not allowed to modify the list directly, since HCatSchema
   *  maintains internal state. Use append/remove to modify the schema.
   */
  public List<HCatFieldSchema> getFields() {
    return Collections.unmodifiableList(this.fieldSchemas);
  }

  /**
   * Note : The position will be re-numbered when one of the preceding columns are removed.
   * Hence, the client should not cache this value and expect it to be always valid.
   * @param fieldName
   * @return the index of field named fieldName in Schema. If field is not
   * present, returns null.
   */
  public Integer getPosition(String fieldName) {
    return fieldPositionMap.get(normalizeName(fieldName));
  }

  public HCatFieldSchema get(String fieldName) throws HCatException {
    return get(getPosition(fieldName));
  }

  public List<String> getFieldNames() {
    return this.fieldNames;
  }

  public HCatFieldSchema get(int position) {
    return fieldSchemas.get(position);
  }

  public int size() {
    return fieldSchemas.size();
  }

  private void reAlignPositionMap(int startPosition, int offset) {
    for (Map.Entry<String, Integer> entry : fieldPositionMap.entrySet()) {
      // Re-align the columns appearing on or after startPostion(say, column 1) such that
      // column 2 becomes column (2+offset), column 3 becomes column (3+offset) and so on.
      Integer entryVal = entry.getValue();
      if (entryVal >= startPosition) {
        entry.setValue(entryVal+offset);
      }
    }
  }

  public void remove(final HCatFieldSchema hcatFieldSchema) throws HCatException {
    if (!fieldSchemas.contains(hcatFieldSchema)) {
      throw new HCatException("Attempt to delete a non-existent column from HCat Schema: " + hcatFieldSchema);
    }     
    fieldSchemas.remove(hcatFieldSchema);
    // Re-align the positionMap by -1 for the columns appearing after hcatFieldSchema.
    String fieldName = normalizeName(hcatFieldSchema.getName());
    reAlignPositionMap(fieldPositionMap.get(fieldName)+1, -1);
    fieldPositionMap.remove(fieldName);
    fieldNames.remove(fieldName);
  }

  private String normalizeName(String name) {
    return name == null ? null : name.toLowerCase();
  }

  @Override
  public String toString() {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for (HCatFieldSchema hfs : fieldSchemas) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      if (hfs.getName() != null) {
        sb.append(hfs.getName());
        sb.append(":");
      }
      sb.append(hfs.toString());
    }
    return sb.toString();
  }

  public String getSchemaAsTypeString() {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for (HCatFieldSchema hfs : fieldSchemas) {
      if (!first) {
        sb.append(",");
      } else {
        first = false;
      }
      if (hfs.getName() != null) {
        sb.append(hfs.getName());
        sb.append(":");
      }
      sb.append(hfs.getTypeString());
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HCatSchema)) {
      return false;
    }
    HCatSchema other = (HCatSchema) obj;
    if (!this.getFields().equals(other.getFields())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
