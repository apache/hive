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

package org.apache.hadoop.hive.registry.state.details;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.registry.state.InbuiltSchemaVersionLifecycleState;
import org.apache.hadoop.hive.registry.state.SchemaVersionLifecycleStates;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InitializedStateDetails implements AbstractStateDetails {

    @JsonProperty
    private MergeInfo mergeInfo = null;

    private InitializedStateDetails() {

    }

    public InitializedStateDetails(String branchName, Long versionId) {
        mergeInfo = new MergeInfo(branchName, versionId);
    }

    @JsonIgnore
    public MergeInfo getMergeInfo() {
        return mergeInfo;
    }

    public void setMergeInfo(MergeInfo mergeInfo) {
        this.mergeInfo = mergeInfo;
    }

    @JsonIgnore
    @Override
    public InbuiltSchemaVersionLifecycleState getStateType() {
        return SchemaVersionLifecycleStates.INITIATED;
    }
}
