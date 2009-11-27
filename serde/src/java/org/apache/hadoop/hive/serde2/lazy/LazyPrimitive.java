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
import org.apache.hadoop.io.Writable;

/**
 * LazyPrimitive stores a primitive Object in a LazyObject.
 */
public abstract class LazyPrimitive<OI extends ObjectInspector, 
    T extends Writable> extends LazyObject<OI> {

  LazyPrimitive(OI oi) {
    super(oi);
  }

  LazyPrimitive(LazyPrimitive<OI, T> copy) {
    super(copy.oi);
    isNull = copy.isNull;
  }

  T data;
  boolean isNull = false;

  /**
   * Returns the primitive object represented by this LazyObject.
   * This is useful because it can make sure we have "null" for null objects.
   */
  public Object getObject() {
    return isNull ? null : this;
  }

  public T getWritableObject() {
    return isNull ? null : data;
  }
  
  public String toString() {
    return isNull ? null : data.toString();
  }
  
  public int hashCode(){
    return isNull ? 0 : data.hashCode();
  }
  
}
