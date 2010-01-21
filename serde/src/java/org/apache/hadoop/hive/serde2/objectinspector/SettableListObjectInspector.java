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
package org.apache.hadoop.hive.serde2.objectinspector;

public interface SettableListObjectInspector extends ListObjectInspector {

  /**
   * Create a list with the given size. All elements will be null.
   * 
   * NOTE: This is different from ArrayList constructor where the argument is
   * capacity. We decided to have size here to allow creation of Java array.
   */
  Object create(int size);

  /**
   * Set the element at index. Returns the list.
   */
  Object set(Object list, int index, Object element);

  /**
   * Resize the list. Returns the list. If the new size is bigger than the
   * current size, new elements will be null. If the new size is smaller than
   * the current size, elements at the end are truncated.
   */
  Object resize(Object list, int newSize);
}
