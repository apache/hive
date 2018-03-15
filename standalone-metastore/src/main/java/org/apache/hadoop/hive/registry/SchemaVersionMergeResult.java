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
package org.apache.hadoop.hive.registry;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchemaVersionMergeResult implements Serializable {
    private static final long serialVersionUID = 1384010779879651612L;

    private SchemaIdVersion schemaIdVersion;
    private String mergeMessage;

    private SchemaVersionMergeResult() {
    }

    public SchemaVersionMergeResult(SchemaIdVersion schemaIdVersion, String mergeMessage) {
        this.schemaIdVersion = schemaIdVersion;
        this.mergeMessage = mergeMessage;
    }

    public SchemaIdVersion getSchemaIdVersion() {
        return schemaIdVersion;
    }

    public String getMergeMessage() {
        return mergeMessage;
    }

    @Override
    public String toString() {
        return "MergeResult{" +
                "schemaIdVersion=" + schemaIdVersion +
                ", mergeMessage='" + mergeMessage + '\'' +
                '}';
    }
}
