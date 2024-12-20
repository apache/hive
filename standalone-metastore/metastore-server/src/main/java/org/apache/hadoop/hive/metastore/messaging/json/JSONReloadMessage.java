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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.messaging.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.metastore.messaging.ReloadMessage;
import org.apache.thrift.TException;

/**
 * JSON implementation of JSONReloadMessage
 */
public class JSONReloadMessage extends ReloadMessage {
    @JsonProperty
    private Long timestamp;

    @JsonProperty
    private String server, servicePrincipal, db, table, tableObjJson, ptnObjJson, refreshEvent;

    /**
     * Default constructor, needed for Jackson.
     */
    public JSONReloadMessage() {
    }

    public JSONReloadMessage(String server, String servicePrincipal, Table tableObj, Partition ptnObj,
                             boolean refreshEvent, Long timestamp) {
        this.server = server;
        this.servicePrincipal = servicePrincipal;

        if (null == tableObj) {
            throw new IllegalArgumentException("Table not valid.");
        }

        this.db = tableObj.getDbName();
        this.table = tableObj.getTableName();

        try {
            this.tableObjJson = MessageBuilder.createTableObjJson(tableObj);
            if (null != ptnObj) {
                this.ptnObjJson = MessageBuilder.createPartitionObjJson(ptnObj);
            } else {
                this.ptnObjJson = null;
            }
        } catch (TException e) {
            throw new IllegalArgumentException("Could not serialize: ", e);
        }

        this.timestamp = timestamp;
        this.refreshEvent = Boolean.toString(refreshEvent);

        checkValid();
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public String getServer() {
        return server;
    }

    @Override
    public String getServicePrincipal() {
        return servicePrincipal;
    }

    @Override
    public String getDB() {
        return db;
    }

    @Override
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean isRefreshEvent() { return Boolean.parseBoolean(refreshEvent); }

    @Override
    public Table getTableObj() throws Exception {
        return (Table) MessageBuilder.getTObj(tableObjJson,Table.class);
    }

    @Override
    public Partition getPtnObj() throws Exception {
        return ((null == ptnObjJson) ? null : (Partition) MessageBuilder.getTObj(ptnObjJson, Partition.class));
    }

    @Override
    public String toString() {
        try {
            return JSONMessageDeserializer.mapper.writeValueAsString(this);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Could not serialize: ", exception);
        }
    }
}
