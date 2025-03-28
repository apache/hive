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

package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;

import java.util.List;

public class GetPartitionsRequestBuilder {
    private String catName = null;
    private String dbName = null;
    private String tblName = null;
    private boolean withAuth = true;
    private String user = null;
    private List<String> groupNames = null;
    private GetProjectionsSpec projectionSpec = null;
    private GetPartitionsFilterSpec filterSpec = null;
    private List<java.lang.String> processorCapabilities = null;
    private String processorIdentifier = null;
    private String validWriteIdList = null;

    public GetPartitionsRequestBuilder(String catName, String dbName, String tblName, boolean withAuth, String user,
                                       List<String> groupNames, GetProjectionsSpec projectionSpec,
                                       GetPartitionsFilterSpec filterSpec, List<String> processorCapabilities,
                                       String processorIdentifier, String validWriteIdList) {
        this.catName = catName;
        this.dbName = dbName;
        this.tblName = tblName;
        this.withAuth = withAuth;
        this.user = user;
        this.groupNames = groupNames;
        this.projectionSpec = projectionSpec;
        this.filterSpec = filterSpec;
        this.processorCapabilities = processorCapabilities;
        this.processorIdentifier = processorIdentifier;
        this.validWriteIdList = validWriteIdList;
    }

    public GetPartitionsRequestBuilder setCatName(String catName) {
        this.catName = catName;
        return this;
    }

    public GetPartitionsRequestBuilder setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public GetPartitionsRequestBuilder setTblName(String tblName) {
        this.tblName = tblName;
        return this;
    }

    public GetPartitionsRequestBuilder setWithAuth(boolean withAuth) {
        this.withAuth = withAuth;
        return this;
    }

    public GetPartitionsRequestBuilder setUser(String user) {
        this.user = user;
        return this;
    }

    public GetPartitionsRequestBuilder setGroupNames(List<String> groupNames) {
        this.groupNames = groupNames;
        return this;
    }

    public GetPartitionsRequestBuilder setProjectionSpec(GetProjectionsSpec projectionSpec) {
        this.projectionSpec = projectionSpec;
        return this;
    }

    public GetPartitionsRequestBuilder setFilterSpec(GetPartitionsFilterSpec filterSpec) {
        this.filterSpec = filterSpec;
        return this;
    }

    public GetPartitionsRequestBuilder setProcessorCapabilities(List<String> processorCapabilities) {
        this.processorCapabilities = processorCapabilities;
        return this;
    }

    public GetPartitionsRequestBuilder setProcessorIdentifier(String processorIdentifier) {
        this.processorIdentifier = processorIdentifier;
        return this;
    }

    public GetPartitionsRequestBuilder setValidWriteIdList(String validWriteIdList) {
        this.validWriteIdList = validWriteIdList;
        return this;
    }

    public GetPartitionsRequest build() {
        GetPartitionsRequest partitionsRequest = new GetPartitionsRequest(dbName, tblName, projectionSpec, filterSpec);
        partitionsRequest.setCatName(catName);
        partitionsRequest.setWithAuth(withAuth);
        partitionsRequest.setUser(user);
        partitionsRequest.setGroupNames(groupNames);
        partitionsRequest.setProcessorCapabilities(processorCapabilities);
        partitionsRequest.setProcessorIdentifier(processorIdentifier);
        partitionsRequest.setValidWriteIdList(validWriteIdList);
        return partitionsRequest;
    }
}