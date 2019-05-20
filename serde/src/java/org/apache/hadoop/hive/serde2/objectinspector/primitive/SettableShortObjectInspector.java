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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

/**
 * A SettableShortObjectInspector can set a short value to an object.
 */
public interface SettableShortObjectInspector extends ShortObjectInspector {

  /**
   * Set the object with the value. Return the object that has the new value.
   * 
   * In most cases the returned value should be the same as o, but in case o is
   * unmodifiable, this will return a new object with new value.
   */
  Object set(Object o, short value);

  /**
   * Create an object with the value.
   */
  Object create(short value);
}
