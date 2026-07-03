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

package org.apache.hadoop.hive.ql.anon.btree;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

public class BtreeListObjectInspector implements ListObjectInspector {

  private ObjectInspector itemOI;

  public BtreeListObjectInspector(final ListTypeInfo info) {
    itemOI = createObjectInspector(info.getListElementTypeInfo());
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return itemOI;
  }

  @Override
  public Object getListElement(Object data, int i) {
    if (data == null || i < 0 || i >= getListLength(data)) {
      return null;
    }
    return ((List) data).get(i);
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    return ((List) data).size();
  }

  @Override
  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }
    return (List) data;
  }

  @Override
  public String getTypeName() {
    return "array<" + itemOI.getTypeName() + ">";
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  static public ObjectInspector createObjectInspector(final TypeInfo info) {
    switch (info.getCategory()) {
      case PRIMITIVE:
        throw new IllegalArgumentException("Unknown primitive type " + ((PrimitiveTypeInfo) info).getPrimitiveCategory());
      case STRUCT:
        return new BtreeObjectInspector((StructTypeInfo) info);
      case LIST:
        return new BtreeListObjectInspector((ListTypeInfo) info);
      default:
        throw new IllegalArgumentException("Unknown type " + info.getCategory());
    }
  }
}
