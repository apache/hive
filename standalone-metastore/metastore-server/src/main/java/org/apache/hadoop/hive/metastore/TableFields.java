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

package org.apache.hadoop.hive.metastore;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.*;

// This class validates the Table fields and creates a mapping to the JDO Column names. When DirectSQL is implemented
// this class should be integrated with the PartitionProjectionEvaluator class. The PartitionProjectionEvaluator class
// should be converted to a tempalte that works for both partition and tables.
public class TableFields {
    // this map stores all the single valued fields in the table class and maps them to the corresponding
    // single-valued fields from the MTable class. This map is used to parse the given table fields.
    private static final ImmutableMap<String, String> allSingleValuedFields = new ImmutableMap.Builder<String, String>()
            .put("id", "id")
            .put("tableName", "tableName")
            .put("dbName", "database.name")
            .put("owner", "owner")
            .put("ownerType", "ownerType")
            .put("createTime", "createTime")
            .put("lastAccessTime", "lastAccessTime")
            .put("retention", "retention")
            .put("sd.location", "sd.location")
            .put("sd.inputFormat", "sd.inputFormat")
            .put("sd.outputFormat", "sd.outputFormat")
            .put("sd.compressed", "sd.isCompressed")
            .put("sd.numBuckets", "sd.numBuckets")
            .put("sd.serdeInfo.name", "sd.serDeInfo.name")
            .put("sd.serdeInfo.serializationLib", "sd.serDeInfo.serializationLib")
            .put("sd.serdeInfo.description", "sd.serDeInfo.description")
            .put("sd.serdeInfo.serializerClass", "sd.serDeInfo.serializerClass")
            .put("sd.serdeInfo.deserializerClass", "sd.serDeInfo.deserializerClass")
            .put("sd.serdeInfo.serdeType", "sd.serDeInfo.serdeType")
            .put("sd.storedAsSubDirectories", "isStoredAsSubDirectories")
            .put("viewOriginalText", "viewOriginalText")
            .put("viewExpandedText", "viewExpandedText")
            .put("rewriteEnabled", "rewriteEnabled")
            .put("tableType", "tableType")
            .put("writeId", "writeId")
            .build();

    private static final ImmutableSet<String> allMultiValuedFields = new ImmutableSet.Builder<String>()
            .add("values")
            .add("sd.cols.name")
            .add("sd.cols.type")
            .add("sd.cols.comment")
            .add("sd.serdeInfo.parameters")
            .add("sd.bucketCols")
            .add("sd.sortCols.col")
            .add("sd.sortCols.order")
            .add("sd.parameters")
            .add("sd.skewedInfo.skewedColNames")
            .add("sd.skewedInfo.skewedColValues")
            .add("sd.skewedInfo.skewedColValueLocationMaps")
            .add("partitionKeys.name")
            .add("partitionKeys.type")
            .add("partitionKeys.comment")
            .add("parameters")
            .build();

    private static final ImmutableSet<String> allFields = new ImmutableSet.Builder<String>()
            .addAll(allSingleValuedFields.keySet())
            .addAll(allMultiValuedFields)
            .build();

    private static void validate(Collection<String> projectionFields) throws MetaException {
        Set<String> verify = new HashSet<>(projectionFields);
        verify.removeAll(allFields);
        if (verify.size() > 0) {
            throw new MetaException("Invalid table fields in the projection spec" + Arrays
                    .toString(verify.toArray(new String[verify.size()])));
        }
    }

    /**
     * Given a list of table fields, checks if all the fields requested are single-valued. If all
     * the fields are single-valued returns list of equivalent MTable fieldnames
     * which can be used in the setResult clause of a JDO query
     *
     * @param fields of tableFields in the projection
     * @return List of JDO field names which can be used in setResult clause
     * of a JDO query. Returns null if input fields cannot be used in a setResult clause
     */
    public static List<String> getMFieldNames(List<String> fields)
            throws MetaException {
        // if there are no fields requested, it means all the fields are requested which include
        // multi-valued fields.
        if (fields == null || fields.isEmpty()) {
            return null;
        }
        // throw exception if there are invalid field names
        validate(fields);
        // else, check if all the fields are single-valued. In case there are multi-valued fields requested
        // return null since setResult in JDO doesn't support multi-valued fields
        if (!allSingleValuedFields.keySet().containsAll(fields)) {
            return null;
        }

        List<String> jdoFields = new ArrayList<>(fields.size());

        for (String field : fields) {
            jdoFields.add(allSingleValuedFields.get(field));
        }
        return jdoFields;
    }
}
