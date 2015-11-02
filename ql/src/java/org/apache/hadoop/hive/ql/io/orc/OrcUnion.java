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
package org.apache.hadoop.hive.ql.io.orc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.orc.OrcProto;

import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory representation of a union type.
 */
final class OrcUnion implements UnionObject {
  private byte tag;
  private Object object;

  void set(byte tag, Object object) {
    this.tag = tag;
    this.object = object;
  }

  @Override
  public byte getTag() {
    return tag;
  }

  @Override
  public Object getObject() {
    return object;
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcUnion.class) {
      return false;
    }
    OrcUnion oth = (OrcUnion) other;
    if (tag != oth.tag) {
      return false;
    } else if (object == null) {
      return oth.object == null;
    } else {
      return object.equals(oth.object);
    }
  }

  @Override
  public int hashCode() {
    int result = tag;
    if (object != null) {
      result ^= object.hashCode();
    }
    return result;
  }

  @Override
  public String toString() {
    return "union(" + Integer.toString(tag & 0xff) + ", " + object + ")";
  }

  static class OrcUnionObjectInspector implements UnionObjectInspector {
    private List<ObjectInspector> children;

    protected OrcUnionObjectInspector() {
      super();
    }
    OrcUnionObjectInspector(int columnId,
                            List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      children = new ArrayList<ObjectInspector>(type.getSubtypesCount());
      for(int i=0; i < type.getSubtypesCount(); ++i) {
        children.add(OrcStruct.createObjectInspector(type.getSubtypes(i),
            types));
      }
    }

    OrcUnionObjectInspector(UnionTypeInfo info) {
      List<TypeInfo> unionChildren = info.getAllUnionObjectTypeInfos();
      this.children = new ArrayList<ObjectInspector>(unionChildren.size());
      for(TypeInfo child: info.getAllUnionObjectTypeInfos()) {
        this.children.add(OrcStruct.createObjectInspector(child));
      }
    }

    @Override
    public List<ObjectInspector> getObjectInspectors() {
      return children;
    }

    @Override
    public byte getTag(Object obj) {
      return ((OrcUnion) obj).tag;
    }

    @Override
    public Object getField(Object obj) {
      return ((OrcUnion) obj).object;
    }

    @Override
    public String getTypeName() {
      StringBuilder builder = new StringBuilder("uniontype<");
      boolean first = true;
      for(ObjectInspector child: children) {
        if (first) {
          first = false;
        } else {
          builder.append(",");
        }
        builder.append(child.getTypeName());
      }
      builder.append(">");
      return builder.toString();
    }

    @Override
    public Category getCategory() {
      return Category.UNION;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        List<ObjectInspector> other = ((OrcUnionObjectInspector) o).children;
        if (other.size() != children.size()) {
          return false;
        }
        for(int i = 0; i < children.size(); ++i) {
          if (!other.get(i).equals(children.get(i))) {
            return false;
          }
        }
        return true;
      }
    }
  }
}
