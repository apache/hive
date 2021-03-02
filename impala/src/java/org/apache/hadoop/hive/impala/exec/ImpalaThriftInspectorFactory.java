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
package org.apache.hadoop.hive.impala.exec;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

public class ImpalaThriftInspectorFactory {
    // CDPD-7084: Investigate caching of Object inspectors for Impala Thrift
    static public AbstractPrimitiveObjectInspector getImpalaThriftObjectInspector(PrimitiveTypeInfo typeInfo)
            throws HiveException {
        switch (typeInfo.getPrimitiveCategory()) {
            case LONG:
                return new ImpalaThriftLongInspector();
            case INT:
                return new ImpalaThriftIntInspector();
            case STRING:
                return new ImpalaThriftStringInspector();
            case TIMESTAMP:
                return new ImpalaThriftTimestampInspector();
            case DECIMAL:
                return new ImpalaThriftDecimalInspector();
            case BOOLEAN:
                return new ImpalaThriftBooleanInspector();
            case SHORT:
                return new ImpalaThriftShortInspector();
            case FLOAT:
                return new ImpalaThriftFloatInspector();
            case DOUBLE:
                return new ImpalaThriftDoubleInspector();
            case DATE:
                return new ImpalaThriftDateInspector();
            case VARCHAR:
                return new ImpalaThriftVarcharInspector();
            case CHAR:
                return new ImpalaThriftCharInspector();
            case BYTE: // tinyint
                return new ImpalaThriftByteInspector();
            case VOID: // null
                return new ImpalaThriftVoidInspector();
            case UNKNOWN: // Not transmitted
            case INTERVAL_YEAR_MONTH: // Not transmitted
            case INTERVAL_DAY_TIME: // Not transmitted
            case TIMESTAMPLOCALTZ: // Impala does not support timestamp with local timezone
            case BINARY: // Impala does not support binary type
            default:
                throw new HiveException("Unhandled primitive type " + typeInfo.getPrimitiveCategory());
        }
    }
}
