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

package org.apache.hadoop.hive.ql.io.protobuf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;

/**
 * SerDe to convert ProtoWritable messages to Hive formats.
 * The serde supports the following properties:
 * <ul>
 *   <li>proto.class: This is required and specifies the class to be used to read the messages.</li>
 *   <li>proto.maptypes: This is optional it declares set of protobuf types that have to be
 *       converted to map objects instead of struct type. It is applied only on a repeated struct
 *       field. The message should have 2 fields, first is used as key and second is used as value.
 *   </li>
 * </ul>
 */
public abstract class ProtobufSerDe extends AbstractSerDe {
  static final String PROTO_CLASS = "proto.class";
  static final String MAP_TYPES = "proto.maptypes";

  protected Class<? extends Message> protoMessageClass;
  private ProtoToHiveConvertor convertor;
  private ObjectInspector objectInspector;
  private Set<String> mapTypes;

  @Override
  public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
      throws SerDeException {
    super.initialize(configuration, tableProperties, partitionProperties);

    this.mapTypes = Sets.newHashSet(properties.getProperty(MAP_TYPES, "").trim().split("\\s*,\\s*"));

    protoMessageClass = loadClass(properties.getProperty(PROTO_CLASS));
    Descriptor descriptor = loadDescriptor(protoMessageClass);

    Map<Descriptor, ObjectInspector> cache = new HashMap<>();
    this.objectInspector = createStructObjectInspector(descriptor, cache);

    Map<Descriptor, ProtoToHiveConvertor> convertorCache = new HashMap<>();
    this.convertor = createConvertor(descriptor, convertorCache);
  }

  private Class<? extends Message> loadClass(String protoClass) throws SerDeException {
    if (protoClass == null) {
      throw new SerDeException(PROTO_CLASS + " has to be set.");
    }
    try {
      Class<?> clazz = getClass().getClassLoader().loadClass(protoClass);
      if (!Message.class.isAssignableFrom(clazz)) {
        throw new SerDeException("Invalid class: " + clazz.getName() + " is not type of: " +
            Message.class.getName());
      }
      @SuppressWarnings("unchecked")
      Class<? extends Message> serdeClass = (Class<? extends Message>) clazz;
      return serdeClass;
    } catch (ClassNotFoundException e) {
      throw new SerDeException("Cannot find/load class: " + protoClass, e);
    }
  }

  private static Descriptor loadDescriptor(Class<? extends Message> protoClass)
      throws SerDeException {
    try {
      Method method = protoClass.getMethod("getDescriptor", (Class<?>[])null);
      return (Descriptor)method.invoke(null, (Object[])null);
    } catch (InvocationTargetException | NoSuchMethodException | SecurityException |
        IllegalAccessException | IllegalArgumentException e) {
      throw new SerDeException("Error trying to get descriptor for class: " + protoClass.getName(),
          e);
    }
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
    throw new UnsupportedOperationException("Not implemented serialize");
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (blob == null) {
      return null;
    }
    Message message = toMessage(blob);
    if (message == null) {
      return null;
    }
    return convertor.convert(message);
  }

  /**
   * Convert the given writable to a message.
   * @param writable The writable object containing the message.
   * @return The converted message object.
   * @throws SerDeException
   */
  protected abstract Message toMessage(Writable writable) throws SerDeException;

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  private ObjectInspector createStructObjectInspector(Descriptor descriptor,
      Map<Descriptor, ObjectInspector> cache) throws SerDeException {
    if (cache.containsKey(descriptor)) {
      return cache.get(descriptor);
    }
    List<String> columnNames = new ArrayList<>();
    List<ObjectInspector> columnOI = new ArrayList<>();
    for (FieldDescriptor field : descriptor.getFields()) {
      columnNames.add(field.getName());
      columnOI.add(createObjectInspector(field, cache));
    }
    ObjectInspector oi = ObjectInspectorFactory.getStandardStructObjectInspector(
        columnNames, columnOI);
    cache.put(descriptor, oi);
    return oi;
  }

  private ObjectInspector createObjectInspector(FieldDescriptor descriptor,
      Map<Descriptor, ObjectInspector> cache) throws SerDeException {
    ObjectInspector oi;
    switch(descriptor.getJavaType()) {
    case BOOLEAN:
      oi = getPrimitive(PrimitiveCategory.BOOLEAN);
      break;
    case BYTE_STRING:
      oi = getPrimitive(PrimitiveCategory.BINARY);
      break;
    case DOUBLE:
      oi = getPrimitive(PrimitiveCategory.DOUBLE);
      break;
    case ENUM:
      oi = getPrimitive(PrimitiveCategory.STRING);
      break;
    case FLOAT:
      oi = getPrimitive(PrimitiveCategory.FLOAT);
      break;
    case INT:
      oi = getPrimitive(PrimitiveCategory.INT);
      break;
    case LONG:
      oi = getPrimitive(PrimitiveCategory.LONG);
      break;
    case STRING:
      oi = getPrimitive(PrimitiveCategory.STRING);
      break;
    case MESSAGE:
      Descriptor msgType = descriptor.getMessageType();
      if (descriptor.isRepeated() && mapTypes.contains(msgType.getFullName())) {
        return getMapObjectInspector(msgType, cache);
      } else {
        oi = createStructObjectInspector(msgType, cache);
      }
      break;
    default:
      throw new IllegalArgumentException("unexpected type: " + descriptor.getJavaType());
    }
    return descriptor.isRepeated() ? ObjectInspectorFactory.getStandardListObjectInspector(oi) : oi;
  }

  private ObjectInspector getMapObjectInspector(Descriptor descriptor,
      Map<Descriptor, ObjectInspector> cache) throws SerDeException {
    List<FieldDescriptor> fields = descriptor.getFields();
    if (fields.size() != 2) {
      throw new SerDeException("Map type " + descriptor.getFullName() +
          " should have only 2 fields, got: " + fields.size());
    }
    ObjectInspector keyOI = createObjectInspector(fields.get(0), cache);
    ObjectInspector valueOI = createObjectInspector(fields.get(1), cache);
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
  }

  private static ObjectInspector getPrimitive(PrimitiveCategory cat) {
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(cat);
  }

  private ProtoToHiveConvertor createConvertor(Descriptor descriptor,
      Map<Descriptor, ProtoToHiveConvertor> cache) throws SerDeException {
    if (cache.containsKey(descriptor)) {
      return cache.get(descriptor);
    }
    List<FieldDescriptor> fields = descriptor.getFields();
    StructConvertor scConvertor = new StructConvertor(fields.size());
    int i = 0;
    for (FieldDescriptor field : descriptor.getFields()) {
      ProtoToHiveConvertor fc;
      if (field.getJavaType() == JavaType.MESSAGE) {
        fc = createConvertor(field.getMessageType(), cache);
      } else if (field.getJavaType() == JavaType.BYTE_STRING) {
        fc = ByteStringConvertor.INSTANCE;
      } else if (field.getJavaType() == JavaType.ENUM) {
        fc = EnumConvertor.INSTANCE;
      } else {
        fc = IdentityConvertor.INSTANCE;
      }
      if (field.isRepeated()) {
        if (field.getJavaType() == JavaType.MESSAGE &&
            mapTypes.contains(field.getMessageType().getFullName())) {
          if (field.getMessageType().getFields().size() != 2) {
            throw new SerDeException("Expected exactly 2 fields for: " +
                field.getMessageType().getFullName());
          }
          fc = new MapConvertor(fc);
        } else {
          fc = new ListConvertor(fc);
        }
      }
      scConvertor.add(i++, field, fc);
    }
    cache.put(descriptor, scConvertor);
    return scConvertor;
  }

  private interface ProtoToHiveConvertor {
    default Object extractAndConvert(FieldDescriptor field, Message msg)  {
      Object val = msg.hasField(field) ? msg.getField(field) : null;
      return val == null ? null : convert(val);
    }

    Object convert(Object obj);
  }

  private static class StructConvertor implements ProtoToHiveConvertor {
    private final FieldDescriptor[] fields;
    private final ProtoToHiveConvertor[] convertors;

    StructConvertor(int size) {
      this.fields = new FieldDescriptor[size];
      this.convertors = new ProtoToHiveConvertor[size];
    }

    void add(int i, FieldDescriptor field, ProtoToHiveConvertor convertor) {
      fields[i] = field;
      convertors[i] = convertor;
    }

    @Override
    public Object convert(Object obj) {
      Message msg = (Message)obj;
      Object[] ret = new Object[fields.length];
      for (int i = 0; i < fields.length; ++i) {
        ret[i] = convertors[i].extractAndConvert(fields[i], msg);
      }
      return ret;
    }
  }

  private static class ListConvertor implements ProtoToHiveConvertor {
    private final ProtoToHiveConvertor convertor;

    ListConvertor(ProtoToHiveConvertor convertor) {
      this.convertor = convertor;
    }

    @Override
    public Object extractAndConvert(FieldDescriptor field, Message msg) {
      int count = msg.getRepeatedFieldCount(field);
      if (count == 0) {
        return null;
      }
      Object[] val = new Object[count];
      for (int j = 0; j < count; ++j) {
        val[j] = convertor.convert(msg.getRepeatedField(field, j));
      }
      return val;
    }

    @Override
    public Object convert(Object obj) {
      throw new UnsupportedOperationException("Use extractAndConvert for ListConvertor");
    }
  }

  private static class MapConvertor implements ProtoToHiveConvertor {
    private final ProtoToHiveConvertor convertor;

    MapConvertor(ProtoToHiveConvertor convertor) {
      this.convertor = convertor;
    }

    @Override
    public Object extractAndConvert(FieldDescriptor field, Message msg) {
      int count = msg.getRepeatedFieldCount(field);
      if (count == 0) {
        return null;
      }
      Map<Object, Object> val = new HashMap<>(count);
      for (int j = 0; j < count; ++j) {
        Object[] entry = (Object[])convertor.convert(msg.getRepeatedField(field, j));
        val.put(entry[0], entry[1]);
      }
      return val;
    }

    @Override
    public Object convert(Object obj) {
      throw new UnsupportedOperationException("Use extractAndConvert for MapConvertor");
    }
  }

  private static class ByteStringConvertor implements ProtoToHiveConvertor {
    private static final ProtoToHiveConvertor INSTANCE = new ByteStringConvertor();

    @Override
    public Object convert(Object obj) {
      return ((ByteString)obj).toByteArray();
    }
  }

  private static class EnumConvertor implements ProtoToHiveConvertor {
    private static final ProtoToHiveConvertor INSTANCE = new EnumConvertor();
    @Override
    public Object convert(Object obj) {
      return ((EnumValueDescriptor)obj).getName();
    }
  }

  private static class IdentityConvertor implements ProtoToHiveConvertor {
    private static final ProtoToHiveConvertor INSTANCE = new IdentityConvertor();

    @Override
    public Object convert(Object obj) {
      return obj;
    }
  }
}
