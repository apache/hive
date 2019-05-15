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
package org.apache.hadoop.hive.ql.exec.persistence;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

@SuppressWarnings("deprecation")
public class MapJoinObjectSerDeContext {
  private final ObjectInspector standardOI;
  private final AbstractSerDe serde;
  private final boolean hasFilter;

  public MapJoinObjectSerDeContext(AbstractSerDe serde, boolean hasFilter)
      throws SerDeException {
    this.serde = serde;
    this.hasFilter = hasFilter;
    this.standardOI = ObjectInspectorUtils.getStandardObjectInspector(serde.getObjectInspector(),
        ObjectInspectorCopyOption.WRITABLE);
  }

  /**
   * @return the standardOI
   */
  public ObjectInspector getStandardOI() {
    return standardOI;
  }

  /**
   * @return the serde
   */
  public AbstractSerDe getSerDe() {
    return serde;
  }

  public boolean hasFilterTag() {
    return hasFilter;
  }

  public String stringify() {
    StandardStructObjectInspector standardStructOI = (StandardStructObjectInspector) standardOI;
    List<? extends StructField> structFields = standardStructOI.getAllStructFieldRefs();
    List<String> typeInfoStrings = new ArrayList<String>();
    for (StructField field : structFields) {
      ObjectInspector fieldOI = field.getFieldObjectInspector();
      typeInfoStrings.add(fieldOI.getTypeName());
    }
    return "[types " + typeInfoStrings.toString() + ", serde=" + serde.getClass().getName()
        + ", hasFilter=" + hasFilter + "]";
  }

  @Override
  public String toString() {
    return "MapJoinObjectSerDeContext [standardOI=" + standardOI + ", serde=" + serde
        + ", hasFilter=" + hasFilter + "]";
  }

}