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

package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * LazyListObjectInspector works on array data that is stored in LazyArray.
 * 
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class LazyListObjectInspector implements ListObjectInspector {

  public static final Log LOG = LogFactory.getLog(LazyListObjectInspector.class
      .getName());

  ObjectInspector listElementObjectInspector;

  byte separator;
  Text nullSequence;
  boolean escaped;
  byte escapeChar;

  /**
   * Call ObjectInspectorFactory.getLazySimpleListObjectInspector instead.
   */
  protected LazyListObjectInspector(ObjectInspector listElementObjectInspector,
      byte separator, Text nullSequence, boolean escaped, byte escapeChar) {
    this.listElementObjectInspector = listElementObjectInspector;
    this.separator = separator;
    this.nullSequence = nullSequence;
    this.escaped = escaped;
    this.escapeChar = escapeChar;
  }

  @Override
  public final Category getCategory() {
    return Category.LIST;
  }

  // without data
  @Override
  public ObjectInspector getListElementObjectInspector() {
    return listElementObjectInspector;
  }

  // with data
  @Override
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }
    LazyArray array = (LazyArray) data;
    return array.getListElementObject(index);
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    LazyArray array = (LazyArray) data;
    return array.getListLength();
  }

  @Override
  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }
    LazyArray array = (LazyArray) data;
    return array.getList();
  }

  @Override
  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "<"
        + listElementObjectInspector.getTypeName() + ">";
  }

  /**
   * Returns the separator for delimiting items in this array. Called by
   * LazyArray.init(...).
   */
  public byte getSeparator() {
    return separator;
  }

  /**
   * Returns the NullSequence for this array. Called by LazyArray.init(...).
   */
  public Text getNullSequence() {
    return nullSequence;
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
  }

}
