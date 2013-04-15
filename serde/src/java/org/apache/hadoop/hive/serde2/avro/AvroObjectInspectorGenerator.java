/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * An AvroObjectInspectorGenerator takes an Avro schema and creates the three
 * data structures Hive needs to work with Avro-encoded data:
 *   * A list of the schema field names
 *   * A list of those fields equivalent types in Hive
 *   * An ObjectInspector capable of working with an instance of that datum.
 */
class AvroObjectInspectorGenerator {
  final private List<String> columnNames;
  final private List<TypeInfo> columnTypes;
  final private ObjectInspector oi;

  public AvroObjectInspectorGenerator(Schema schema) throws SerDeException {
    verifySchemaIsARecord(schema);

    this.columnNames = generateColumnNames(schema);
    this.columnTypes = SchemaToTypeInfo.generateColumnTypes(schema);
    assert columnNames.size() == columnTypes.size();
    this.oi = createObjectInspector();
  }

  private void verifySchemaIsARecord(Schema schema) throws SerDeException {
    if(!schema.getType().equals(Schema.Type.RECORD))
      throw new AvroSerdeException("Schema for table must be of type RECORD. " +
          "Received type: " + schema.getType());
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<TypeInfo> getColumnTypes() {
    return columnTypes;
  }

  public ObjectInspector getObjectInspector() {
    return oi;
  }

  private ObjectInspector createObjectInspector() throws SerDeException {
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());

    // At this point we've verified the types are correct.
    for(int i = 0; i < columnNames.size(); i++) {
      columnOIs.add(i, createObjectInspectorWorker(columnTypes.get(i)));
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);
  }

  private ObjectInspector createObjectInspectorWorker(TypeInfo ti) throws SerDeException {
    // We don't need to do the check for U[T,Null] here because we'll give the real type
    // at deserialization and the object inspector will never see the actual union.
    if(!supportedCategories(ti))
      throw new AvroSerdeException("Don't yet support this type: " + ti);
    ObjectInspector result;
    switch(ti.getCategory()) {
      case PRIMITIVE:
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo)ti;
        result = PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(pti.getPrimitiveCategory());
        break;
      case STRUCT:
        StructTypeInfo sti = (StructTypeInfo)ti;
        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>(sti.getAllStructFieldTypeInfos().size());
        for(TypeInfo typeInfo : sti.getAllStructFieldTypeInfos()) {
          ois.add(createObjectInspectorWorker(typeInfo));
        }

        result = ObjectInspectorFactory
                .getStandardStructObjectInspector(sti.getAllStructFieldNames(), ois);

        break;
      case MAP:
        MapTypeInfo mti = (MapTypeInfo)ti;
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory
                    .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING),
            createObjectInspectorWorker(mti.getMapValueTypeInfo()));
        break;
      case LIST:
        ListTypeInfo ati = (ListTypeInfo)ti;
        result = ObjectInspectorFactory
                .getStandardListObjectInspector(createObjectInspectorWorker(ati.getListElementTypeInfo()));
        break;
      case UNION:
        UnionTypeInfo uti = (UnionTypeInfo)ti;
        List<TypeInfo> allUnionObjectTypeInfos = uti.getAllUnionObjectTypeInfos();
        List<ObjectInspector> unionObjectInspectors = new ArrayList<ObjectInspector>(allUnionObjectTypeInfos.size());

        for (TypeInfo typeInfo : allUnionObjectTypeInfos) {
          unionObjectInspectors.add(createObjectInspectorWorker(typeInfo));
        }

        result = ObjectInspectorFactory.getStandardUnionObjectInspector(unionObjectInspectors);
        break;
      default:
        throw new AvroSerdeException("No Hive categories matched: " + ti);
    }

    return result;
  }

  private boolean supportedCategories(TypeInfo ti) {
    final ObjectInspector.Category c = ti.getCategory();
    return c.equals(ObjectInspector.Category.PRIMITIVE) ||
           c.equals(ObjectInspector.Category.MAP)       ||
           c.equals(ObjectInspector.Category.LIST)      ||
           c.equals(ObjectInspector.Category.STRUCT)    ||
           c.equals(ObjectInspector.Category.UNION);
  }

  private List<String> generateColumnNames(Schema schema) {
    List<Schema.Field> fields = schema.getFields();
    List<String> fieldsList = new ArrayList<String>(fields.size());

    for (Schema.Field field : fields) {
      fieldsList.add(field.name());
    }

    return fieldsList;
  }

}
