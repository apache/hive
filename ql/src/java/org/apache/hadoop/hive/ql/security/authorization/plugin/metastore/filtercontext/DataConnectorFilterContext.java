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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.filtercontext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataConnectorFilterContext extends HiveMetaStoreAuthorizableEvent {

    private static final Logger LOG = LoggerFactory.getLogger(DataConnectorFilterContext.class);

    List<String> connectors = null;

    public DataConnectorFilterContext(List<String> connectors) {
        super(null);
        this.connectors = connectors;
        getAuthzContext();
    }

    @Override
    public HiveMetaStoreAuthzInfo getAuthzContext() {
        HiveMetaStoreAuthzInfo ret =
                new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.QUERY, getInputHObjs(), getOutputHObjs(), null);
        return ret;
    }

    private List<HivePrivilegeObject> getInputHObjs() {
        LOG.debug("==> DataConnectorFilterContext.getInputHObjs()");

        List<HivePrivilegeObject> ret = new ArrayList<>();
        for (String connector : connectors) {
            HivePrivilegeObject.HivePrivilegeObjectType type = HivePrivilegeObject.HivePrivilegeObjectType.DATACONNECTOR;
            HivePrivilegeObject.HivePrivObjectActionType objectActionType =
                    HivePrivilegeObject.HivePrivObjectActionType.OTHER;
            HivePrivilegeObject hivePrivilegeObject =
                    new HivePrivilegeObject(type, null, connector, null, null, objectActionType, null, null);
            ret.add(hivePrivilegeObject);
        }
        LOG.debug("<== DataConnectorFilterContext.getInputHObjs(): ret=" + ret);

        return ret;
    }

    private List<HivePrivilegeObject> getOutputHObjs() {
        return Collections.emptyList();
    }

    public List<String> getDataConnectors() {
        return connectors;
    }
}