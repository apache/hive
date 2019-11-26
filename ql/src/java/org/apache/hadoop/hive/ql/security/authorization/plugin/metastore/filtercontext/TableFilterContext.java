
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
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivObjectActionType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizableEvent;
import org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthzInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableFilterContext extends HiveMetaStoreAuthorizableEvent {
    private static final Log LOG = LogFactory.getLog(DatabaseFilterContext.class);

    List<Table>  tables     = null;
    List<String> tableNames = null;
    String       dbName     = null;

    public TableFilterContext(List<Table> tables) {
        super(null);
        this.tables = tables;
        getAuthzContext();
    }

    public TableFilterContext(String dbName, List<String> tableNames) {
        super(null);
        this.dbName     = dbName;
        this.tableNames = tableNames;
    }

    @Override
    public HiveMetaStoreAuthzInfo getAuthzContext() {
        HiveMetaStoreAuthzInfo ret = new HiveMetaStoreAuthzInfo(preEventContext, HiveOperationType.QUERY, getInputHObjs(), getOutputHObjs(), null);
        return ret;
    }

    private List<HivePrivilegeObject> getInputHObjs() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> TableFilterContext.getOutputHObjs()");
        }

        List<HivePrivilegeObject> ret   = new ArrayList<>();

        if  (tables != null) {
            for (Table table : tables) {
                HivePrivilegeObjectType type = HivePrivilegeObjectType.TABLE_OR_VIEW;
                HivePrivObjectActionType objectActionType = HivePrivilegeObject.HivePrivObjectActionType.OTHER;
                HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(type, table.getDbName(), table.getTableName(), null, null, objectActionType, null, null);
                ret.add(hivePrivilegeObject);
            }
        } else {
            for (String tableName : tableNames) {
                HivePrivilegeObjectType type = HivePrivilegeObjectType.TABLE_OR_VIEW;
                HivePrivObjectActionType objectActionType = HivePrivilegeObject.HivePrivObjectActionType.OTHER;
                HivePrivilegeObject hivePrivilegeObject = new HivePrivilegeObject(type, dbName, tableName, null, null, objectActionType, null, null);
                ret.add(hivePrivilegeObject);
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== TableFilterContext.getOutputHObjs(): ret=" + ret);
        }

        return ret;
    }

    private List<HivePrivilegeObject> getOutputHObjs() { return Collections.emptyList(); }

    public List<Table> getTables() {
        return tables;
    }
}
