/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ReloadEvent;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;

/**
 * IcebergEventListener is a Hive Metastore event listener that invalidates the cache
 * of the HMSCachingCatalog when certain events occur, such as altering or dropping a table.
 */
public class HMSEventListener extends MetaStoreEventListener {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(HMSEventListener.class);
    /**
     * Constructor for HMSEventListener.
     *
     * @param config the configuration to use for the listener
     */
    public HMSEventListener(Configuration config) {
        super(config);
    }


    private Catalog getCatalog() {
       return HMSCatalogFactory.getLastCatalog();
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        Catalog catalog = getCatalog();
        if (catalog instanceof HMSCachingCatalog hmsCachingCatalog) {
            String dbName = event.getOldTable().getDbName();
            String tableName = event.getOldTable().getTableName();
            LOG.debug("onAlterTable: invalidating table cache for {}.{}", dbName, tableName);
            hmsCachingCatalog.invalidateTable(dbName, tableName);
        }
    }

    @Override
    public void onDropTable(DropTableEvent event) {
        Catalog catalog = getCatalog();
        if (catalog instanceof HMSCachingCatalog hmsCachingCatalog) {
            String dbName = event.getTable().getDbName();
            String tableName = event.getTable().getTableName();
            LOG.debug("onDropTable: invalidating table cache for {}.{}", dbName, tableName);
            hmsCachingCatalog.invalidateTable(dbName, tableName);
        }
    }

    @Override
    public void onReload(ReloadEvent reloadEvent) {
        Catalog catalog = getCatalog();
        if (catalog instanceof HMSCachingCatalog hmsCachingCatalog) {
            Table tableObj = reloadEvent.getTableObj();
            String dbName = tableObj.getDbName();
            String tableName = tableObj.getTableName();
            LOG.debug("onReload: invalidating table cache for {}.{}", dbName, tableName);
            hmsCachingCatalog.invalidateTable(dbName, tableName);
        }
    }

    @Override
    public void onDropDatabase (DropDatabaseEvent dbEvent) throws MetaException {
        Catalog catalog = getCatalog();
        if (catalog instanceof HMSCachingCatalog hmsCachingCatalog) {
            String dbName = dbEvent.getDatabase().getName();
            LOG.debug("onDropDatabase: invalidating tables cache for {}", dbName);
            hmsCachingCatalog.invalidateNamespace(dbName);
        }
    }

}
