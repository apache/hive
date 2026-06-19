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

import java.util.Comparator;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.NullValueOption;

/**
 * This class wraps the ObjectInspectorUtils.compare method and implements java.util.Comparator.
 */
public class ObjectComparator implements Comparator<Object> {

  private final ObjectInspector objectInspector1;
  private final ObjectInspector objectInspector2;
  private final NullValueOption nullSortOrder;
  private final MapEqualComparer mapEqualComparer = new FullMapEqualComparer();

  public ObjectComparator(ObjectInspector objectInspector1, ObjectInspector objectInspector2,
                          NullValueOption nullSortOrder) {
    this.objectInspector1 = objectInspector1;
    this.objectInspector2 = objectInspector2;
    this.nullSortOrder = nullSortOrder;
  }

  @Override
  public int compare(Object o1, Object o2) {
    return ObjectInspectorUtils.compare(o1, objectInspector1, o2, objectInspector2, mapEqualComparer, nullSortOrder);
  }
}
