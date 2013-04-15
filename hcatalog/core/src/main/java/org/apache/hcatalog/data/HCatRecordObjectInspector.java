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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.data;

import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class HCatRecordObjectInspector extends StandardStructObjectInspector {

    protected HCatRecordObjectInspector(List<String> structFieldNames,
                                        List<ObjectInspector> structFieldObjectInspectors) {
        super(structFieldNames, structFieldObjectInspectors);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
        if (data == null) {
            return new IllegalArgumentException("Data passed in to get field from was null!");
        }

        int fieldID = ((MyField) fieldRef).getFieldID();
        if (!(fieldID >= 0 && fieldID < fields.size())) {
            throw new IllegalArgumentException("Invalid field index [" + fieldID + "]");
        }

        return ((HCatRecord) data).get(fieldID);
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object o) {
        return ((HCatRecord) o).getAll();
    }

}
