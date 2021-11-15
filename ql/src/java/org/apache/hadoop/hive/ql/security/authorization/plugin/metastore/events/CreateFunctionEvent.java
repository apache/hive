/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.events;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.events.PreCreateFunctionEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 Authorizable Event for HiveMetaStore operation CreateFunction
 */
public class CreateFunctionEvent extends HiveMetaStoreAuthorizableEvent {
    private static final Logger LOG = LoggerFactory.getLogger(CreateFunctionEvent.class);

    private String COMMAND_STR = "create function";

    public CreateFunctionEvent(PreEventContext preEventContext) {
        super(preEventContext);
    }

    @Override
    public HiveMetaStoreAuthzInfo getAuthzContext() {
        HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.CREATEFUNCTION, getInputHObjs(), getOutputHObjs(), COMMAND_STR);

        return ret;
    }

    private List<HivePrivilegeObject> getInputHObjs() { return Collections.emptyList(); }

    private List<HivePrivilegeObject> getOutputHObjs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> CreateFunctionEvent.getOutputHObjs()");
        }

        List<HivePrivilegeObject> ret   = new ArrayList<>();
        PreCreateFunctionEvent event = (PreCreateFunctionEvent) preEventContext;
        Function function = event.getFunction();
        List<ResourceUri> uris   = function.getResourceUris();
        ret.add(new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DATABASE, function.getDbName(), null, null, null,
                HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, null,
                function.getOwnerName(), function.getOwnerType()));
        ret.add(new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.FUNCTION, function.getDbName(), function.getFunctionName(), null,
                null, HivePrivilegeObject.HivePrivObjectActionType.OTHER, null, function.getClassName(), function.getOwnerName(), function.getOwnerType()));

        if (uris != null && !uris.isEmpty()) {
            for(ResourceUri uri: uris) {
                ret.add(new HivePrivilegeObject(HivePrivilegeObject.HivePrivilegeObjectType.DFS_URI, null, uri.getUri()));
            }
        }

        COMMAND_STR = buildCommandString(function);

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== CreateFunctionEvent.getOutputHObjs(): ret=" + ret);
        }

        return ret;
    }

    private String buildCommandString(Function function) {
        String ret = COMMAND_STR;
        if (function != null) {
            String functionName = function.getFunctionName();
            ret = ret + (StringUtils.isNotEmpty(functionName)? " " + functionName : "");
        }
        return ret;
    }
}
