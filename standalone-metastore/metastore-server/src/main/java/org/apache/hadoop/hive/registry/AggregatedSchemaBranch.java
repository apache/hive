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
import java.util.Collection;


/**
 *   This class represents the aggregated information about the schema branch which includes all the schema versions tied
 *   to that branch and the schema version from which the branch was created
 */

@JsonIgnoreProperties(ignoreUnknown = true)
public class AggregatedSchemaBranch implements Serializable {

    private static final long serialVersionUID = -4013250198049024761L;

    private SchemaBranch schemaBranch;
    private Long rootSchemaVersion;
    private Collection<SchemaVersionInfo> schemaVersionInfos;

    private AggregatedSchemaBranch() {

    }

    public AggregatedSchemaBranch(SchemaBranch schemaBranch, Long rootSchemaVersion, Collection<SchemaVersionInfo> schemaVersionInfos) {
        this.schemaBranch = schemaBranch;
        this.rootSchemaVersion = rootSchemaVersion;
        this.schemaVersionInfos = schemaVersionInfos;
    }

    public SchemaBranch getSchemaBranch() {
        return schemaBranch;
    }

    public Long getRootSchemaVersion() {
        return rootSchemaVersion;
    }

    public Collection<SchemaVersionInfo> getSchemaVersionInfos() {
        return schemaVersionInfos;
    }

    @Override
    public String toString() {
        return "AggregatedSchemaBranch{" +
                "schemaBranch=" + schemaBranch +
                ", rootSchemaVersion=" + rootSchemaVersion +
                ", schemaVersionInfos=" + schemaVersionInfos +
                '}';
    }
}
