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
package org.apache.hadoop.hive.serde2.lazybinary.objectinspector;

import java.util.List;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * ObjectInspector for LazyBinaryList.
 */
public class LazyBinaryListObjectInspector extends StandardListObjectInspector {

  protected LazyBinaryListObjectInspector() {
    super();
  }
  protected LazyBinaryListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    super(listElementObjectInspector);
  }

  @Override
  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }
    LazyBinaryArray array = (LazyBinaryArray) data;
    return array.getList();
  }

  @Override
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }
    LazyBinaryArray array = (LazyBinaryArray) data;
    return array.getListElementObject(index);
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    LazyBinaryArray array = (LazyBinaryArray) data;
    return array.getListLength();
  }
}
