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
package org.apache.hcatalog.mapreduce;

import java.io.Serializable;
import java.util.List;

import org.apache.hcatalog.data.schema.HCatSchema;

/** The class used to serialize and store the information read from the metadata server */
public class JobInfo implements Serializable{

    /** The serialization version */
    private static final long serialVersionUID = 1L;

    /** The db and table names. */
    private final String dbName;
    private final String tableName;

    /** The table schema. */
    private final HCatSchema tableSchema;

    /** The list of partitions matching the filter. */
    private final List<PartInfo> partitions;

    /**
     * Instantiates a new howl job info.
     * @param tableName the table name
     * @param tableSchema the table schema
     * @param partitions the partitions
     */
    public JobInfo(HCatTableInfo howlTableInfo, HCatSchema tableSchema,
            List<PartInfo> partitions) {
        this.tableName = howlTableInfo.getTableName();
        this.dbName = howlTableInfo.getDatabaseName();
        this.tableSchema = tableSchema;
        this.partitions = partitions;
    }

    /**
     * Gets the value of dbName
     * @return the dbName
     */
    public String getDatabaseName() {
        return tableName;
    }

    /**
     * Gets the value of tableName
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Gets the value of tableSchema
     * @return the tableSchema
     */
    public HCatSchema getTableSchema() {
        return tableSchema;
    }

    /**
     * Gets the value of partitions
     * @return the partitions
     */
    public List<PartInfo> getPartitions() {
        return partitions;
    }

}
