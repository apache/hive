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
package org.apache.hadoop.hive.serde2.lazybinary.objectinspector;

import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUnion;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;

import java.util.List;

/**
 * ObjectInspector for LazyBinaryUnion.
 *
 * @see org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUnion
 */
public class LazyBinaryUnionObjectInspector extends
    StandardUnionObjectInspector {

  protected LazyBinaryUnionObjectInspector() {
    super();
  }
  protected LazyBinaryUnionObjectInspector(List<ObjectInspector> unionFieldObjectInspectors) {
    super(unionFieldObjectInspectors);
  }

  /**
   * Return the tag of the object.
   */
  public byte getTag(Object o) {
    if (o == null) {
      return -1;
    }
    LazyBinaryUnion lazyBinaryUnion = (LazyBinaryUnion) o;
    return lazyBinaryUnion.getTag();
  }

  /**
   * Return the field based on the tag value associated with the Object.
   */
  public Object getField(Object o) {
    if (o == null) {
      return null;
    }
    LazyBinaryUnion lazyBinaryUnion = (LazyBinaryUnion) o;
    return lazyBinaryUnion.getField();
  }
}
