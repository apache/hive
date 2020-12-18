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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.service.rpc.thrift.TColumnValue;

public class ImpalaThriftIntInspector extends AbstractPrimitiveObjectInspector implements IntObjectInspector {

    ImpalaThriftIntInspector() {
        super(TypeInfoFactory.intTypeInfo);
    }

    @Override
    public Object getPrimitiveWritableObject(Object o) {
        throw new UnsupportedOperationException("getPrimitiveWritableObject is not currently implemented");
    }

    @Override
    public Object getPrimitiveJavaObject(Object o) {
        TColumnValue value = (TColumnValue) o;
        return ImpalaResultInspector.toColumnValue(value);
    }

    @Override
    public Object copyObject(Object o) {
        return o;
    }

    @Override
    public boolean preferWritable() {
        return false;
    }

    @Override
    public int get(Object o) {
        TColumnValue v = (TColumnValue) o;
        return (Integer) ImpalaResultInspector.toColumnValue(v);
    }
}
