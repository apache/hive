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

package org.apache.hadoop.hive.serde2;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class CustomNonSettableListObjectInspector1  implements ListObjectInspector {

  private ObjectInspector listElementObjectInspector;

  protected CustomNonSettableListObjectInspector1() {
    super();
  }
  protected CustomNonSettableListObjectInspector1(
      ObjectInspector listElementObjectInspector) {
    this.listElementObjectInspector = listElementObjectInspector;
  }

  public final Category getCategory() {
    return Category.LIST;
  }

  // without data
  public ObjectInspector getListElementObjectInspector() {
    return listElementObjectInspector;
  }

  // Not supported for the test case
  public Object getListElement(Object data, int index) {
    return null;
  }

  // Not supported for the test case
  public int getListLength(Object data) {
    return 0;
  }

  // Not supported for the test case
  public List<?> getList(Object data) {
    return null;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "<"
        + listElementObjectInspector.getTypeName() + ">";
  }
}

