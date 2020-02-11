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

import java.util.List;

/**
 * A StandardListObjectInspector which also implements the
 * ConstantObjectInspector interface.
 *
 * Always use the ObjectInspectorFactory to create new ObjectInspector objects,
 * instead of directly creating an instance of this class.
 */
public class StandardConstantListObjectInspector extends StandardListObjectInspector
  implements ConstantObjectInspector {

  private List<?> value;

  protected StandardConstantListObjectInspector() {
    super();
  }
  /**
   * Call ObjectInspectorFactory.getStandardListObjectInspector instead.
   */
  protected StandardConstantListObjectInspector(
      ObjectInspector listElementObjectInspector, List<?> value) {
    super(listElementObjectInspector);
    this.value = value;
  }

  @Override
  public List<?> getWritableConstantValue() {
    return value;
  }
}
