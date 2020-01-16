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

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.service.rpc.thrift.TColumnValue;

public class ImpalaThriftTimestampInspector extends AbstractPrimitiveObjectInspector
        implements TimestampObjectInspector {

    ImpalaThriftTimestampInspector() {
        super(TypeInfoFactory.timestampTypeInfo);
    }

    @Override
    public TimestampWritableV2 getPrimitiveWritableObject(Object o) {
        throw new UnsupportedOperationException("getPrimitiveWritableObject is not currently implemented");
    }

    @Override
    public Timestamp getPrimitiveJavaObject(Object o) {
        TColumnValue value = (TColumnValue) o;
        return ImpalaResultInspector.getTimestampValue(value.getStringVal());
    }

    @Override
    public Object copyObject(Object o) {
        return o;
    }

    @Override
    public boolean preferWritable() {
        return false;
    }
}
