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

package org.apache.hcatalog.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hive.hbase.HBaseSerDe;
import org.apache.hadoop.hive.hbase.LazyHBaseRow;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hcatalog.common.HCatConstants;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Implementation of ResultConverter using HBaseSerDe
 * mapping between HBase schema and HCatRecord schema is defined by
 * {@link HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY}
 */
class HBaseSerDeResultConverter implements  ResultConverter {

    private HBaseSerDe serDe;
    private HCatSchema schema;
    private HCatSchema outputSchema;
    private StructObjectInspector hCatRecordOI;
    private StructObjectInspector lazyHBaseRowOI;
    private String hbaseColumnMapping;
    private final Long outputVersion;

    /**
     * @param schema table schema
     * @param outputSchema schema of projected output
     * @param hcatProperties table properties
     * @throws IOException thrown if hive's HBaseSerDe couldn't be initialized
     */
    HBaseSerDeResultConverter(HCatSchema schema,
                                     HCatSchema outputSchema,
                                     Properties hcatProperties) throws IOException {
        this(schema,outputSchema,hcatProperties,null);
    }

    /**
     * @param schema table schema
     * @param outputSchema schema of projected output
     * @param hcatProperties table properties
     * @param outputVersion value to write in timestamp field
     * @throws IOException thrown if hive's HBaseSerDe couldn't be initialized
     */
    HBaseSerDeResultConverter(HCatSchema schema,
                                     HCatSchema outputSchema,
                                     Properties hcatProperties,
                                     Long outputVersion) throws IOException {

        hbaseColumnMapping =  hcatProperties.getProperty(HBaseConstants.PROPERTY_COLUMN_MAPPING_KEY);
        hcatProperties.setProperty(HBaseSerDe.HBASE_COLUMNS_MAPPING,hbaseColumnMapping);

        this.outputVersion = outputVersion;

        this.schema = schema;
        if(outputSchema == null) {
            this.outputSchema = schema;
        }
        else {
            this.outputSchema = outputSchema;
        }

        hCatRecordOI = createStructObjectInspector();
        try {
            serDe = new HBaseSerDe();
            serDe.initialize(new Configuration(),hcatProperties);
            lazyHBaseRowOI = (StructObjectInspector) serDe.getObjectInspector();
        } catch (SerDeException e) {
            throw new IOException("SerDe initialization failed",e);
        }
    }

    @Override
    public Put convert(HCatRecord record) throws IOException {
        try {
            //small hack to explicitly specify timestamp/version number to use
            //since HBaseSerDe does not support specifying it
            //will have to decide whether we will write our own or contribute code
            //for the SerDe
            Put put = (Put)serDe.serialize(record.getAll(),hCatRecordOI);
            Put res;
            if(outputVersion == null) {
                res = put;
            }
            else {
                res = new Put(put.getRow(),outputVersion.longValue());
                for(List<KeyValue> row: put.getFamilyMap().values()) {
                    for(KeyValue el: row) {
                        res.add(el.getFamily(),el.getQualifier(),el.getValue());
                    }
                }
            }
            return res;
        } catch (SerDeException e) {
            throw new IOException("serialization failed",e);
        }
    }

    @Override
    public HCatRecord convert(Result result) throws IOException {
        // Deserialize bytesRefArray into struct and then convert that struct to
        // HCatRecord.
        LazyHBaseRow struct;
        try {
            struct = (LazyHBaseRow)serDe.deserialize(result);
        } catch (SerDeException e) {
            throw new IOException(e);
        }

        List<Object> outList = new ArrayList<Object>(outputSchema.size());

        String colName;
        Integer index;

        for(HCatFieldSchema col : outputSchema.getFields()){

            colName = col.getName().toLowerCase();
            index = outputSchema.getPosition(colName);

            if(index != null){
                StructField field = lazyHBaseRowOI.getStructFieldRef(colName);
                outList.add(getTypedObj(lazyHBaseRowOI.getStructFieldData(struct, field), field.getFieldObjectInspector()));
            }
        }
        return new DefaultHCatRecord(outList);
    }

    private Object getTypedObj(Object data, ObjectInspector oi) throws IOException{
        // The real work-horse method. We are gobbling up all the laziness benefits
        // of Hive-LazyHBaseRow by deserializing everything and creating crisp  HCatRecord
        // with crisp Java objects inside it. We have to do it because higher layer
        // may not know how to do it.
        //TODO leverage laziness of SerDe
        switch(oi.getCategory()){

            case PRIMITIVE:
                return ((PrimitiveObjectInspector)oi).getPrimitiveJavaObject(data);

            case MAP:
                MapObjectInspector moi = (MapObjectInspector)oi;
                Map<?,?> lazyMap = moi.getMap(data);
                ObjectInspector keyOI = moi.getMapKeyObjectInspector();
                ObjectInspector valOI = moi.getMapValueObjectInspector();
                Map<Object,Object> typedMap = new HashMap<Object,Object>(lazyMap.size());
                for(Map.Entry<?,?> e : lazyMap.entrySet()){
                    typedMap.put(getTypedObj(e.getKey(), keyOI), getTypedObj(e.getValue(), valOI));
                }
                return typedMap;

            case LIST:
                ListObjectInspector loi = (ListObjectInspector)oi;
                List<?> lazyList = loi.getList(data);
                ObjectInspector elemOI = loi.getListElementObjectInspector();
                List<Object> typedList = new ArrayList<Object>(lazyList.size());
                Iterator<?> itr = lazyList.listIterator();
                while(itr.hasNext()){
                    typedList.add(getTypedObj(itr.next(),elemOI));
                }
                return typedList;

            case STRUCT:
                StructObjectInspector soi = (StructObjectInspector)oi;
                List<? extends StructField> fields = soi.getAllStructFieldRefs();
                List<Object> typedStruct = new ArrayList<Object>(fields.size());
                for(StructField field : fields){
                    typedStruct.add( getTypedObj(soi.getStructFieldData(data, field), field.getFieldObjectInspector()));
                }
                return typedStruct;


            default:
                throw new IOException("Don't know how to deserialize: "+oi.getCategory());

        }
    }

    private StructObjectInspector createStructObjectInspector() throws IOException {

        if( outputSchema == null ) {
            throw new IOException("Invalid output schema specified");
        }

        List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
        List<String> fieldNames = new ArrayList<String>();

        for(HCatFieldSchema hcatFieldSchema : outputSchema.getFields()) {
            TypeInfo type = TypeInfoUtils.getTypeInfoFromTypeString(hcatFieldSchema.getTypeString());

            fieldNames.add(hcatFieldSchema.getName());
            fieldInspectors.add(getObjectInspector(type));
        }

        StructObjectInspector structInspector = ObjectInspectorFactory.
                getStandardStructObjectInspector(fieldNames, fieldInspectors);
        return structInspector;
    }

    private ObjectInspector getObjectInspector(TypeInfo type) throws IOException {

        switch(type.getCategory()) {

            case PRIMITIVE :
                PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
                return PrimitiveObjectInspectorFactory.
                        getPrimitiveJavaObjectInspector(primitiveType.getPrimitiveCategory());

            case MAP :
                MapTypeInfo mapType = (MapTypeInfo) type;
                MapObjectInspector mapInspector = ObjectInspectorFactory.getStandardMapObjectInspector(
                        getObjectInspector(mapType.getMapKeyTypeInfo()), getObjectInspector(mapType.getMapValueTypeInfo()));
                return mapInspector;

            case LIST :
                ListTypeInfo listType = (ListTypeInfo) type;
                ListObjectInspector listInspector = ObjectInspectorFactory.getStandardListObjectInspector(
                        getObjectInspector(listType.getListElementTypeInfo()));
                return listInspector;

            case STRUCT :
                StructTypeInfo structType = (StructTypeInfo) type;
                List<TypeInfo> fieldTypes = structType.getAllStructFieldTypeInfos();

                List<ObjectInspector> fieldInspectors = new ArrayList<ObjectInspector>();
                for(TypeInfo fieldType : fieldTypes) {
                    fieldInspectors.add(getObjectInspector(fieldType));
                }

                StructObjectInspector structInspector = ObjectInspectorFactory.getStandardStructObjectInspector(
                        structType.getAllStructFieldNames(), fieldInspectors);
                return structInspector;

            default :
                throw new IOException("Unknown field schema type");
        }
    }

    public String getHBaseScanColumns() throws IOException {
        StringBuilder sb = new StringBuilder();
        if(hbaseColumnMapping == null){
            throw new IOException("HBase column mapping found to be null.");
        }

        List<String> outputFieldNames = this.outputSchema.getFieldNames();
        List<Integer> outputColumnMapping = new ArrayList<Integer>();
        for(String fieldName: outputFieldNames){
            int position = schema.getPosition(fieldName);
            outputColumnMapping.add(position);
        }

        try {
            List<String> columnFamilies = new ArrayList<String>();
            List<String> columnQualifiers = new ArrayList<String>();
            HBaseSerDe.parseColumnMapping(hbaseColumnMapping, columnFamilies, null, columnQualifiers, null);
            for(int i = 0; i < outputColumnMapping.size(); i++){
                int cfIndex = outputColumnMapping.get(i);
                String cf = columnFamilies.get(cfIndex);
                // We skip the key column.
                if (cf.equals(HBaseSerDe.HBASE_KEY_COL) == false) {
                    String qualifier = columnQualifiers.get(i);
                    sb.append(cf);
                    sb.append(":");
                    if (qualifier != null) {
                        sb.append(qualifier);
                    }
                    sb.append(" ");
                }
            }

        } catch (SerDeException e) {

            throw new IOException(e);
        }

        return sb.toString();
    }
}
