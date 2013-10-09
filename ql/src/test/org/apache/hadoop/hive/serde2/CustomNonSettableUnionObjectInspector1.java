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

package org.apache.hadoop.hive.serde2;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;

public class CustomNonSettableUnionObjectInspector1 implements UnionObjectInspector{

  private final List<ObjectInspector> children;

  public CustomNonSettableUnionObjectInspector1(List<ObjectInspector> children) {
    this.children = children;
  }

  public static class StandardUnion implements UnionObject {
    protected byte tag;
    protected Object object;

    public StandardUnion() {
    }

    public StandardUnion(byte tag, Object object) {
      this.tag = tag;
      this.object = object;
    }

    @Override
    public Object getObject() {
      return object;
    }

    @Override
    public byte getTag() {
      return tag;
    }

    @Override
    public String toString() {
      return tag + ":" + object;
    }
  }

  /**
   * Return the tag of the object.
   */
  public byte getTag(Object o) {
    if (o == null) {
      return -1;
    }
    return ((UnionObject) o).getTag();
  }

  /**
   * Return the field based on the tag value associated with the Object.
   */
  public Object getField(Object o) {
    if (o == null) {
      return null;
    }
    return ((UnionObject) o).getObject();
  }

  public Category getCategory() {
    return Category.UNION;
  }

  public String getTypeName() {
    return null;
  }

  @Override
  public String toString() {
    return null;
  }

  @Override
  public List<ObjectInspector> getObjectInspectors() {
    return children;
  }
}
