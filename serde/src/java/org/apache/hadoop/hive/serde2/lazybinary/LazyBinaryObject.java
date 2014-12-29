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
package org.apache.hadoop.hive.serde2.lazybinary;

import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * LazyBinaryObject stores an object in a binary format in a byte[]. For
 * example, a double takes four bytes.
 * 
 * A LazyBinaryObject can represent any primitive object or hierarchical object
 * like string, list, map or struct.
 */
public abstract class LazyBinaryObject<OI extends ObjectInspector> implements LazyObjectBase {

  OI oi;

  /**
   * Create a LazyBinaryObject.
   * 
   * @param oi
   *          Derived classes can access meta information about this Lazy Binary
   *          Object (e.g, length, null-bits) from it.
   */
  protected LazyBinaryObject(OI oi) {
    this.oi = oi;
  }

  public void setNull() {
    throw new IllegalStateException("should not be called");
  }

  @Override
  public abstract int hashCode();
}
