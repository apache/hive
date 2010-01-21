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

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;

/**
 * Defines a LazyBinaryPrimitive.
 * 
 * data will be initialized to corresponding types in different LazyBinary
 * primitive classes. For example, data will be a BooleanWritable in the
 * LazyBinaryBoolean class.
 * 
 * There is no null flag any more,
 * 
 */
public abstract class LazyBinaryPrimitive<OI extends ObjectInspector, T extends Writable>
    extends LazyBinaryObject<OI> {

  LazyBinaryPrimitive(OI oi) {
    super(oi);
  }

  LazyBinaryPrimitive(LazyBinaryPrimitive<OI, T> copy) {
    super(copy.oi);
  }

  T data;

  /**
   * Returns the primitive object represented by this LazyBinaryObject. This is
   * useful because it can make sure we have "null" for null objects.
   */
  @Override
  public Object getObject() {
    return data;
  }

  public T getWritableObject() {
    return data;
  }

  @Override
  public String toString() {
    return data.toString();
  }

  @Override
  public int hashCode() {
    return data == null ? 0 : data.hashCode();
  }
}
