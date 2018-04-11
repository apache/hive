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

package org.apache.hadoop.hive.serde2.dynamic_type;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

/**
 * DynamicSerDeFieldList.
 *
 */
public class DynamicSerDeFieldList extends DynamicSerDeSimpleNode implements
    Serializable {

  // private void writeObject(ObjectOutputStream out) throws IOException {
  // out.writeObject(types_by_column_name);
  // out.writeObject(ordered_types);
  // }

  // production: Field()*

  // mapping of the fieldid to the field
  private Map<Integer, DynamicSerDeTypeBase> types_by_id = null;
  private Map<String, DynamicSerDeTypeBase> types_by_column_name = null;
  private DynamicSerDeTypeBase[] ordered_types = null;

  private Map<String, Integer> ordered_column_id_by_name = null;

  public DynamicSerDeFieldList(int i) {
    super(i);
  }

  public DynamicSerDeFieldList(thrift_grammar p, int i) {
    super(p, i);
  }

  private DynamicSerDeField getField(int i) {
    return (DynamicSerDeField) jjtGetChild(i);
  }

  public final DynamicSerDeField[] getChildren() {
    int size = jjtGetNumChildren();
    DynamicSerDeField[] result = new DynamicSerDeField[size];
    for (int i = 0; i < size; i++) {
      result[i] = (DynamicSerDeField) jjtGetChild(i);
    }
    return result;
  }

  private int getNumFields() {
    return jjtGetNumChildren();
  }

  public void initialize() {
    if (types_by_id == null) {
      // multiple means of lookup
      types_by_id = new HashMap<Integer, DynamicSerDeTypeBase>();
      types_by_column_name = new HashMap<String, DynamicSerDeTypeBase>();
      ordered_types = new DynamicSerDeTypeBase[jjtGetNumChildren()];
      ordered_column_id_by_name = new HashMap<String, Integer>();

      // put them in and also roll them up while we're at it
      // a Field contains a FieldType which in turn contains a type
      for (int i = 0; i < jjtGetNumChildren(); i++) {
        DynamicSerDeField mt = getField(i);
        DynamicSerDeTypeBase type = mt.getFieldType().getMyType();
        // types get initialized in case they need to setup any
        // internal data structures - e.g., DynamicSerDeStructBase
        type.initialize();
        type.fieldid = mt.fieldid;
        type.name = mt.name;

        types_by_id.put(Integer.valueOf(mt.fieldid), type);
        types_by_column_name.put(mt.name, type);
        ordered_types[i] = type;
        ordered_column_id_by_name.put(mt.name, i);
      }
    }
  }

  private DynamicSerDeTypeBase getFieldByFieldId(int i) {
    return types_by_id.get(i);
  }

  protected DynamicSerDeTypeBase getFieldByName(String fieldname) {
    return types_by_column_name.get(fieldname);
  }

  /**
   * Indicates whether fields can be out of order or missing. i.e., is it really
   * real thrift serialization. This is used by dynamicserde to do some
   * optimizations if it knows all the fields exist and are required and are
   * serialized in order. For now, those optimizations are only done for
   * DynamicSerDe serialized data so always set to false for now.
   */
  protected boolean isRealThrift = false;

  protected boolean[] fieldsPresent;

  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    ArrayList<Object> struct = null;

    if (reuse == null) {
      struct = new ArrayList<Object>(getNumFields());
      for (DynamicSerDeTypeBase orderedType : ordered_types) {
        struct.add(null);
      }
    } else {
      struct = (ArrayList<Object>) reuse;
      assert (struct.size() == ordered_types.length);
    }

    boolean fastSkips = iprot instanceof org.apache.hadoop.hive.serde2.thrift.SkippableTProtocol;

    // may need to strip away the STOP marker when in thrift mode
    boolean stopSeen = false;

    if (fieldsPresent == null) {
      fieldsPresent = new boolean[ordered_types.length];
    }
    Arrays.fill(fieldsPresent, false);

    // Read the fields.
    for (int i = 0; i < getNumFields(); i++) {
      DynamicSerDeTypeBase mt = null;
      TField field = null;

      if (!isRealThrift && getField(i).isSkippable()) {
        // PRE - all the fields are required and serialized in order - is
        // !isRealThrift
        mt = ordered_types[i];
        if (fastSkips) {
          ((org.apache.hadoop.hive.serde2.thrift.SkippableTProtocol) iprot)
              .skip(mt.getType());
        } else {
          TProtocolUtil.skip(iprot, mt.getType());
        }
        struct.set(i, null);
        continue;
      }
      if (thrift_mode) {
        field = iprot.readFieldBegin();

        if (field.type >= 0) {
          if (field.type == TType.STOP) {
            stopSeen = true;
            break;
          }
          mt = getFieldByFieldId(field.id);
          if (mt == null) {
            System.err.println("ERROR for fieldid: " + field.id
                + " system has no knowledge of this field which is of type : "
                + field.type);
            TProtocolUtil.skip(iprot, field.type);
            continue;
          }
        }
      }

      // field.type < 0 means that this is a faked Thrift field, e.g.,
      // TControlSeparatedProtocol, which does not
      // serialize the field id in the stream. As a result, the only way to get
      // the field id is to fall back to
      // the position "i".
      // The intention of this hack (field.type < 0) is to make
      // TControlSeparatedProtocol a real Thrift prototype,
      // but there are a lot additional work to do to fulfill that, and that
      // protocol inherently does not support
      // versioning (adding/deleting fields).
      int orderedId = -1;
      if (!thrift_mode || field.type < 0) {
        mt = ordered_types[i];
        // We don't need to lookup order_column_id_by_name because we know it
        // must be "i".
        orderedId = i;
      } else {
        // Set the correct position
        orderedId = ordered_column_id_by_name.get(mt.name);
      }
      struct.set(orderedId, mt.deserialize(struct.get(orderedId), iprot));
      if (thrift_mode) {
        iprot.readFieldEnd();
      }
      fieldsPresent[orderedId] = true;
    }

    for (int i = 0; i < ordered_types.length; i++) {
      if (!fieldsPresent[i]) {
        struct.set(i, null);
      }
    }

    if (thrift_mode && !stopSeen) {
      // strip off the STOP marker, which may be left if all the fields were in
      // the serialization
      iprot.readFieldBegin();
    }
    return struct;
  }

  TField field = new TField();

  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException, IllegalAccessException {
    // Assuming the ObjectInspector represents exactly the same type as this
    // struct.
    // This assumption should be checked during query compile time.
    assert (oi instanceof StructObjectInspector);
    StructObjectInspector soi = (StructObjectInspector) oi;

    boolean writeNulls = oprot instanceof org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol;

    // For every field
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    if (fields.size() != ordered_types.length) {
      throw new SerDeException("Trying to serialize " + fields.size()
          + " fields into a struct with " + ordered_types.length + " object="
          + o + " objectinspector=" + oi.getTypeName());
    }
    for (int i = 0; i < fields.size(); i++) {
      Object f = soi.getStructFieldData(o, fields.get(i));
      DynamicSerDeTypeBase mt = ordered_types[i];

      if (f == null && !writeNulls) {
        continue;
      }

      if (thrift_mode) {
        field = new TField(mt.name, mt.getType(), (short) mt.fieldid);
        oprot.writeFieldBegin(field);
      }

      if (f == null) {
        ((org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol) oprot)
            .writeNull();
      } else {
        mt.serialize(f, fields.get(i).getFieldObjectInspector(), oprot);
      }
      if (thrift_mode) {
        oprot.writeFieldEnd();
      }
    }
    if (thrift_mode) {
      oprot.writeFieldStop();
    }
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    String prefix = "";
    for (DynamicSerDeField t : getChildren()) {
      result.append(prefix + t.fieldid + ":"
          + t.getFieldType().getMyType().toString() + " " + t.name);
      prefix = ",";
    }
    return result.toString();
  }
}
