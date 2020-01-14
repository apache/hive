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
package org.apache.hadoop.hive.ql.exec.impala;

import org.apache.hadoop.hive.serde2.BaseStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hive.service.rpc.thrift.TBoolValue;
import org.apache.hive.service.rpc.thrift.TByteValue;
import org.apache.hive.service.rpc.thrift.TDoubleValue;
import org.apache.hive.service.rpc.thrift.TI16Value;
import org.apache.hive.service.rpc.thrift.TI32Value;
import org.apache.hive.service.rpc.thrift.TI64Value;
import org.apache.hive.service.rpc.thrift.TRow;
import org.apache.hive.service.rpc.thrift.TStringValue;
import org.apache.hive.service.rpc.thrift.TColumnValue;

import java.util.List;

/**
 * An implementation of BaseStructObjectInspector that allows for the inspection and/or conversion of results streamed
 * from an Impala coordinators thrift interface. It relies on the fact that Impala implements the same thrift interface
 * for row values as Hive.
 */
class ImpalaResultInspector extends BaseStructObjectInspector {
    public ImpalaResultInspector(List<String> structFieldNames,
                                 List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    public ImpalaResultInspector(List<String> structFieldNames,
                                 List<ObjectInspector> structFieldObjectInspectors,
                                 List<String> structFieldComments) {
        super(structFieldNames, structFieldObjectInspectors, structFieldComments);
    }

    // CDPD-7084: Investigate caching of Object inspectors for Impala Thrift
    public static ImpalaResultInspector getImpalaResultInspector(List<String> structFieldNames,
                                                                 List<ObjectInspector> structFieldObjectInspectors) {
        return new ImpalaResultInspector(structFieldNames, structFieldObjectInspectors);
    }

    private static Boolean getBooleanValue(TBoolValue tBoolValue) {
        if (tBoolValue.isSetValue()) {
            return tBoolValue.isValue();
        }
        return null;
    }

    private static Byte getByteValue(TByteValue tByteValue) {
        if (tByteValue.isSetValue()) {
            return tByteValue.getValue();
        }
        return null;
    }

    private static Short getShortValue(TI16Value tI16Value) {
        if (tI16Value.isSetValue()) {
            return tI16Value.getValue();
        }
        return null;
    }

    private static Integer getIntegerValue(TI32Value tI32Value) {
        if (tI32Value.isSetValue()) {
            return tI32Value.getValue();
        }
        return null;
    }

    private static Long getLongValue(TI64Value tI64Value) {
        if (tI64Value.isSetValue()) {
            return tI64Value.getValue();
        }
        return null;
    }

    private static Double getDoubleValue(TDoubleValue tDoubleValue) {
        if (tDoubleValue.isSetValue()) {
            return tDoubleValue.getValue();
        }
        return null;
    }

    private static String getStringValue(TStringValue tStringValue) {
        if (tStringValue.isSetValue()) {
            return tStringValue.getValue();
        }
        return null;
    }

    public static Object toColumnValue(TColumnValue value) {
        TColumnValue._Fields field = value.getSetField();
        switch (field) {
            case BOOL_VAL:
                return getBooleanValue(value.getBoolVal());
            case BYTE_VAL:
                return getByteValue(value.getByteVal());
            case I16_VAL:
                return getShortValue(value.getI16Val());
            case I32_VAL:
                return getIntegerValue(value.getI32Val());
            case I64_VAL:
                return getLongValue(value.getI64Val());
            case DOUBLE_VAL:
                return getDoubleValue(value.getDoubleVal());
            case STRING_VAL:
                return getStringValue(value.getStringVal());
        }
        throw new IllegalArgumentException("Unsupported column type");
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (data == null) {
            return null;
        }
        TRow row = (TRow) data;
        MyField f = (MyField) fieldRef;

        int fieldID = f.getFieldID();
        assert (fieldID >= 0 && fieldID < fields.size());

        return row.getColVals().get(fieldID);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
        throw new UnsupportedOperationException("getStructFieldDataAsList is not implemented.");
    }
}
