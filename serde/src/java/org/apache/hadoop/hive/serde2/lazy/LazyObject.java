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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * LazyObject stores an object in a range of bytes in a byte[].
 *
 * A LazyObject can represent any primitive object or hierarchical object like
 * array, map or struct.
 */
public abstract class LazyObject<OI extends ObjectInspector> extends LazyObjectBase {

  protected OI oi;

  /**
   * Create a LazyObject.
   *
   * @param oi
   *          Derived classes can access meta information about this Lazy Object
   *          (e.g, separator, nullSequence, escaper) from it.
   */
  protected LazyObject(OI oi) {
    this.oi = oi;
  }

  @Override
  public abstract int hashCode();

  protected OI getInspector() {
    return oi;
  }

  protected void setInspector(OI oi) {
    this.oi = oi;
  }
}
