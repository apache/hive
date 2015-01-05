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
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/**
 * ObjectInspectorConverters.
 *
 */
public final class ObjectInspectorConverters {

  /**
   * A converter which will convert objects with one ObjectInspector to another.
   */
  public static interface Converter {
    Object convert(Object input);
  }

  /**
   * IdentityConverter.
   *
   */
  public static class IdentityConverter implements Converter {
    public Object convert(Object input) {
      return input;
    }
  }

  private static Converter getConverter(PrimitiveObjectInspector inputOI,
      PrimitiveObjectInspector outputOI) {
    switch (outputOI.getPrimitiveCategory()) {
    case BOOLEAN:
      return new PrimitiveObjectInspectorConverter.BooleanConverter(
          inputOI,
          (SettableBooleanObjectInspector) outputOI);
    case BYTE:
      return new PrimitiveObjectInspectorConverter.ByteConverter(
          inputOI,
          (SettableByteObjectInspector) outputOI);
    case SHORT:
      return new PrimitiveObjectInspectorConverter.ShortConverter(
          inputOI,
          (SettableShortObjectInspector) outputOI);
    case INT:
      return new PrimitiveObjectInspectorConverter.IntConverter(
          inputOI,
          (SettableIntObjectInspector) outputOI);
    case LONG:
      return new PrimitiveObjectInspectorConverter.LongConverter(
          inputOI,
          (SettableLongObjectInspector) outputOI);
    case FLOAT:
      return new PrimitiveObjectInspectorConverter.FloatConverter(
          inputOI,
          (SettableFloatObjectInspector) outputOI);
    case DOUBLE:
      return new PrimitiveObjectInspectorConverter.DoubleConverter(
          inputOI,
          (SettableDoubleObjectInspector) outputOI);
    case STRING:
      if (outputOI instanceof WritableStringObjectInspector) {
        return new PrimitiveObjectInspectorConverter.TextConverter(
            inputOI);
      } else if (outputOI instanceof JavaStringObjectInspector) {
        return new PrimitiveObjectInspectorConverter.StringConverter(
            inputOI);
      }
    case CHAR:
      return new PrimitiveObjectInspectorConverter.HiveCharConverter(
          inputOI,
          (SettableHiveCharObjectInspector) outputOI);
    case VARCHAR:
      return new PrimitiveObjectInspectorConverter.HiveVarcharConverter(
          inputOI,
          (SettableHiveVarcharObjectInspector) outputOI);
    case DATE:
      return new PrimitiveObjectInspectorConverter.DateConverter(
          inputOI,
          (SettableDateObjectInspector) outputOI);
    case TIMESTAMP:
      return new PrimitiveObjectInspectorConverter.TimestampConverter(
          inputOI,
          (SettableTimestampObjectInspector) outputOI);
    case BINARY:
      return new PrimitiveObjectInspectorConverter.BinaryConverter(
          inputOI,
          (SettableBinaryObjectInspector)outputOI);
    case DECIMAL:
      return new PrimitiveObjectInspectorConverter.HiveDecimalConverter(
          (PrimitiveObjectInspector) inputOI,
          (SettableHiveDecimalObjectInspector) outputOI);
    default:
      throw new RuntimeException("Hive internal error: conversion of "
          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
          + " not supported yet.");
    }
  }

  /**
   * Returns a converter that converts objects from one OI to another OI. The
   * returned (converted) object belongs to this converter, so that it can be
   * reused across different calls.
   */
  public static Converter getConverter(ObjectInspector inputOI,
      ObjectInspector outputOI) {
    // If the inputOI is the same as the outputOI, just return an
    // IdentityConverter.
    if (inputOI.equals(outputOI)) {
      return new IdentityConverter();
    }
    switch (outputOI.getCategory()) {
    case PRIMITIVE:
      return getConverter((PrimitiveObjectInspector) inputOI, (PrimitiveObjectInspector) outputOI);
    case STRUCT:
      return new StructConverter(inputOI,
          (SettableStructObjectInspector) outputOI);
    case LIST:
      return new ListConverter(inputOI,
          (SettableListObjectInspector) outputOI);
    case MAP:
      return new MapConverter(inputOI,
          (SettableMapObjectInspector) outputOI);
    case UNION:
      return new UnionConverter(inputOI,
          (SettableUnionObjectInspector) outputOI);
    default:
      throw new RuntimeException("Hive internal error: conversion of "
          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
          + " not supported yet.");
    }
  }

  /*
   * getConvertedOI with caching to store settable properties of the object
   * inspector. Caching might help when the object inspector
   * contains complex nested data types. Caching is not explicitly required for
   * the returned object inspector across multiple invocations since the
   * ObjectInspectorFactory already takes care of it.
   */
  public static ObjectInspector getConvertedOI(
      ObjectInspector inputOI, ObjectInspector outputOI,
      Map<ObjectInspector, Boolean> oiSettableProperties
      ) {
    return getConvertedOI(inputOI, outputOI, oiSettableProperties, true);
  }

  /*
   * getConvertedOI without any caching.
   */
  public static ObjectInspector getConvertedOI(
      ObjectInspector inputOI,
      ObjectInspector outputOI
      ) {
    return getConvertedOI(inputOI, outputOI, null, true);
  }

  /**
   * Utility function to convert from one object inspector type to another.
   * The output object inspector type should have all fields as settableOI type.
   * The above condition can be violated only if equalsCheck is true and inputOI is
   * equal to outputOI.
   * @param inputOI : input object inspector
   * @param outputOI : output object inspector
   * @param oiSettableProperties : The object inspector to isSettable mapping used to cache
   *                               intermediate results.
   * @param equalsCheck : Do we need to check if the inputOI and outputOI are the same?
   *                      true : If they are the same, we return the object inspector directly.
   *                      false : Do not perform an equality check on inputOI and outputOI
   * @return : The output object inspector containing all settable fields. The return value
   *           can contain non-settable fields only if inputOI equals outputOI and equalsCheck is
   *           true.
   */
  public static ObjectInspector getConvertedOI(
      ObjectInspector inputOI,
      ObjectInspector outputOI,
      Map<ObjectInspector, Boolean> oiSettableProperties,
      boolean equalsCheck) {
    // 1. If equalsCheck is true and the inputOI is the same as the outputOI OR
    // 2. If the outputOI has all fields settable, return it
    if ((equalsCheck && inputOI.equals(outputOI)) ||
        ObjectInspectorUtils.hasAllFieldsSettable(outputOI, oiSettableProperties) == true) {
      return outputOI;
    }
    // Return the settable equivalent object inspector for primitive categories
    // For eg: for table T containing partitions p1 and p2 (possibly different
    // from the table T), return the settable inspector for T. The inspector for
    // T is settable recursively i.e all the nested fields are also settable.
    switch (outputOI.getCategory()) {
    case PRIMITIVE:
      // Create a writable object inspector for primitive type and return it.
      PrimitiveObjectInspector primOutputOI = (PrimitiveObjectInspector) outputOI;
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
          (PrimitiveTypeInfo)primOutputOI.getTypeInfo());
    case STRUCT:
      StructObjectInspector structOutputOI = (StructObjectInspector) outputOI;
      // create a standard settable struct object inspector.
      List<? extends StructField> listFields = structOutputOI.getAllStructFieldRefs();
      List<String> structFieldNames = new ArrayList<String>(listFields.size());
      List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(
          listFields.size());

      for (StructField listField : listFields) {
        structFieldNames.add(listField.getFieldName());
        // We need to make sure that the underlying fields are settable as well.
        // Hence, the recursive call for each field.
        // Note that equalsCheck is false while invoking getConvertedOI() because
        // we need to bypass the initial inputOI.equals(outputOI) check.
        structFieldObjectInspectors.add(getConvertedOI(listField.getFieldObjectInspector(),
            listField.getFieldObjectInspector(), oiSettableProperties, false));
      }
      return ObjectInspectorFactory.getStandardStructObjectInspector(
          structFieldNames,
          structFieldObjectInspectors);
    case LIST:
      ListObjectInspector listOutputOI = (ListObjectInspector) outputOI;
      // We need to make sure that the list element type is settable.
      return ObjectInspectorFactory.getStandardListObjectInspector(
          getConvertedOI(listOutputOI.getListElementObjectInspector(),
              listOutputOI.getListElementObjectInspector(), oiSettableProperties, false));
    case MAP:
      MapObjectInspector mapOutputOI = (MapObjectInspector) outputOI;
      // We need to make sure that the key type and the value types are settable.
      return ObjectInspectorFactory.getStandardMapObjectInspector(
          getConvertedOI(mapOutputOI.getMapKeyObjectInspector(),
              mapOutputOI.getMapKeyObjectInspector(), oiSettableProperties, false),
          getConvertedOI(mapOutputOI.getMapValueObjectInspector(),
              mapOutputOI.getMapValueObjectInspector(), oiSettableProperties, false));
    case UNION:
      UnionObjectInspector unionOutputOI = (UnionObjectInspector) outputOI;
      // create a standard settable union object inspector
      List<ObjectInspector> unionListFields = unionOutputOI.getObjectInspectors();
      List<ObjectInspector> unionFieldObjectInspectors = new ArrayList<ObjectInspector>(
          unionListFields.size());
      for (ObjectInspector listField : unionListFields) {
        // We need to make sure that all the field associated with the union are settable.
        unionFieldObjectInspectors.add(getConvertedOI(listField, listField, oiSettableProperties,
            false));
      }
      return ObjectInspectorFactory.getStandardUnionObjectInspector(unionFieldObjectInspectors);
    default:
      // Unsupported in-memory structure.
      throw new RuntimeException("Hive internal error: conversion of "
          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
          + " not supported yet.");
    }
  }

  /**
   * A converter class for List.
   */
  public static class ListConverter implements Converter {

    ListObjectInspector inputOI;
    SettableListObjectInspector outputOI;

    ObjectInspector inputElementOI;
    ObjectInspector outputElementOI;

    ArrayList<Converter> elementConverters;

    Object output;

    public ListConverter(ObjectInspector inputOI,
        SettableListObjectInspector outputOI) {
      if (inputOI instanceof ListObjectInspector) {
        this.inputOI = (ListObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputElementOI = this.inputOI.getListElementObjectInspector();
        outputElementOI = outputOI.getListElementObjectInspector();
        output = outputOI.create(0);
        elementConverters = new ArrayList<Converter>();
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      // Create enough elementConverters
      // NOTE: we have to have a separate elementConverter for each element,
      // because the elementConverters can reuse the internal object.
      // So it's not safe to use the same elementConverter to convert multiple
      // elements.
      int size = inputOI.getListLength(input);
      while (elementConverters.size() < size) {
        elementConverters.add(getConverter(inputElementOI, outputElementOI));
      }

      // Convert the elements
      outputOI.resize(output, size);
      for (int index = 0; index < size; index++) {
        Object inputElement = inputOI.getListElement(input, index);
        Object outputElement = elementConverters.get(index).convert(
            inputElement);
        outputOI.set(output, index, outputElement);
      }
      return output;
    }

  }

  /**
   * A converter class for Struct.
   */
  public static class StructConverter implements Converter {

    StructObjectInspector inputOI;
    SettableStructObjectInspector outputOI;

    List<? extends StructField> inputFields;
    List<? extends StructField> outputFields;

    ArrayList<Converter> fieldConverters;

    Object output;

    public StructConverter(ObjectInspector inputOI,
        SettableStructObjectInspector outputOI) {
      if (inputOI instanceof StructObjectInspector) {
        this.inputOI = (StructObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputFields = this.inputOI.getAllStructFieldRefs();
        outputFields = outputOI.getAllStructFieldRefs();

        // If the output has some extra fields, set them to NULL.
        int minFields = Math.min(inputFields.size(), outputFields.size());
        fieldConverters = new ArrayList<Converter>(minFields);
        for (int f = 0; f < minFields; f++) {
          fieldConverters.add(getConverter(inputFields.get(f)
              .getFieldObjectInspector(), outputFields.get(f)
              .getFieldObjectInspector()));
        }
        output = outputOI.create();
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      int minFields = Math.min(inputFields.size(), outputFields.size());
      // Convert the fields
      for (int f = 0; f < minFields; f++) {
        Object inputFieldValue = inputOI.getStructFieldData(input, inputFields.get(f));
        Object outputFieldValue = fieldConverters.get(f).convert(inputFieldValue);
        outputOI.setStructFieldData(output, outputFields.get(f), outputFieldValue);
      }

      // set the extra fields to null
      for (int f = minFields; f < outputFields.size(); f++) {
        outputOI.setStructFieldData(output, outputFields.get(f), null);
      }

      return output;
    }
  }

  /**
   * A converter class for Union.
   */
  public static class UnionConverter implements Converter {

    UnionObjectInspector inputOI;
    SettableUnionObjectInspector outputOI;

    List<? extends ObjectInspector> inputFields;
    List<? extends ObjectInspector> outputFields;

    ArrayList<Converter> fieldConverters;

    Object output;

    public UnionConverter(ObjectInspector inputOI,
        SettableUnionObjectInspector outputOI) {
      if (inputOI instanceof UnionObjectInspector) {
        this.inputOI = (UnionObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputFields = this.inputOI.getObjectInspectors();
        outputFields = outputOI.getObjectInspectors();

        // If the output has some extra fields, set them to NULL in convert().
        int minFields = Math.min(inputFields.size(), outputFields.size());
        fieldConverters = new ArrayList<Converter>(minFields);
        for (int f = 0; f < minFields; f++) {
          fieldConverters.add(getConverter(inputFields.get(f), outputFields.get(f)));
        }

        // Create an empty output object which will be populated when convert() is invoked.
        output = outputOI.create();
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      int minFields = Math.min(inputFields.size(), outputFields.size());
      // Convert the fields
      for (int f = 0; f < minFields; f++) {
        Object outputFieldValue = fieldConverters.get(f).convert(inputOI);
        outputOI.addField(output, (ObjectInspector)outputFieldValue);
      }

      // set the extra fields to null
      for (int f = minFields; f < outputFields.size(); f++) {
        outputOI.addField(output, null);
      }

      return output;
    }
  }

  /**
   * A converter class for Map.
   */
  public static class MapConverter implements Converter {

    MapObjectInspector inputOI;
    SettableMapObjectInspector outputOI;

    ObjectInspector inputKeyOI;
    ObjectInspector outputKeyOI;

    ObjectInspector inputValueOI;
    ObjectInspector outputValueOI;

    ArrayList<Converter> keyConverters;
    ArrayList<Converter> valueConverters;

    Object output;

    public MapConverter(ObjectInspector inputOI,
        SettableMapObjectInspector outputOI) {
      if (inputOI instanceof MapObjectInspector) {
        this.inputOI = (MapObjectInspector)inputOI;
        this.outputOI = outputOI;
        inputKeyOI = this.inputOI.getMapKeyObjectInspector();
        outputKeyOI = outputOI.getMapKeyObjectInspector();
        inputValueOI = this.inputOI.getMapValueObjectInspector();
        outputValueOI = outputOI.getMapValueObjectInspector();
        keyConverters = new ArrayList<Converter>();
        valueConverters = new ArrayList<Converter>();
        output = outputOI.create();
      } else if (!(inputOI instanceof VoidObjectInspector)) {
        throw new RuntimeException("Hive internal error: conversion of " +
            inputOI.getTypeName() + " to " + outputOI.getTypeName() +
            "not supported yet.");
      }
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      // Create enough keyConverters/valueConverters
      // NOTE: we have to have a separate key/valueConverter for each key/value,
      // because the key/valueConverters can reuse the internal object.
      // So it's not safe to use the same key/valueConverter to convert multiple
      // key/values.

      // NOTE: This code tries to get all key-value pairs out of the map.
      // It's not very efficient. The more efficient way should be to let MapOI
      // return an Iterator. This is currently not supported by MapOI yet.

      Map<?, ?> map = inputOI.getMap(input);
      int size = map.size();

      while (keyConverters.size() < size) {
        keyConverters.add(getConverter(inputKeyOI, outputKeyOI));
        valueConverters.add(getConverter(inputValueOI, outputValueOI));
      }

      // CLear the output
      outputOI.clear(output);

      // Convert the key/value pairs
      int entryID = 0;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object inputKey = entry.getKey();
        Object inputValue = entry.getValue();
        Object outputKey = keyConverters.get(entryID).convert(inputKey);
        Object outputValue = valueConverters.get(entryID).convert(inputValue);
        entryID++;
        outputOI.put(output, outputKey, outputValue);
      }
      return output;
    }

  }

  private ObjectInspectorConverters() {
    // prevent instantiation
  }

}
