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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseFilterContext extends  HiveMetaStoreAuthorizableEvent {

    private static final Log LOG = LogFactory.getLog(DatabaseFilterContext.class);

    List<String> databases = null;

    public DatabaseFilterContext(List<String> databases) {
        super(null);
        this.databases = databases;
        getAuthzContext();
    }

    @Override
    public HiveMetaStoreAuthzInfo getAuthzContext() {
        HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.QUERY, getInputHObjs(), getOutputHObjs(), null);
        return ret;
    }

    private List<HivePrivilegeObject> getInputHObjs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> DatabaseFilterContext.getOutputHObjs()");
        }

        List<HivePrivilegeObject> ret   = new ArrayList<>();
        for(String database: databases) {
            HivePrivilegeObjectType  type                = HivePrivilegeObjectType.DATABASE;
            HivePrivObjectActionType objectActionType    = HivePrivObjectActionType.OTHER;
            HivePrivilegeObject      hivePrivilegeObject = new HivePrivilegeObject(type, database, null, null, null, objectActionType, null, null);
            ret.add(hivePrivilegeObject);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== DatabaseFilterContext.getOutputHObjs(): ret=" + ret);
        }

        return ret;
    }

    private List<HivePrivilegeObject> getOutputHObjs() { return Collections.emptyList(); }

    public List<String> getDatabases() {
        return databases;
    }
}