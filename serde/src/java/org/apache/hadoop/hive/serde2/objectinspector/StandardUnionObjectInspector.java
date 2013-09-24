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

import java.util.ArrayList;
import java.util.List;

/**
 * StandardUnionObjectInspector works on union data that is stored as
 * UnionObject.
 * It holds the list of the object inspectors corresponding to each type of the
 * object the Union can hold. The UniobObject has tag followed by the object
 * it is holding.
 *
 * Always use the {@link ObjectInspectorFactory} to create new ObjectInspector
 * objects, instead of directly creating an instance of this class.
 */
public class StandardUnionObjectInspector extends SettableUnionObjectInspector {
  private List<ObjectInspector> ois;

  protected StandardUnionObjectInspector() {
    super();
  }
  public StandardUnionObjectInspector(List<ObjectInspector> ois) {
    this.ois = ois;
  }

  public List<ObjectInspector> getObjectInspectors() {
    return ois;
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

    public void setObject(Object o) {
      this.object = o;
    }

    public void setTag(byte tag) {
      this.tag = tag;
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
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append(getTypeName());
    return sb.toString();
  }

  @Override
  public Object create() {
    ArrayList<Object> a = new ArrayList<Object>();
    return a;
  }

  @Override
  public Object addField(Object union, ObjectInspector oi) {
    ArrayList<Object> a = (ArrayList<Object>) union;
    a.add(oi);
    return a;
  }

}
