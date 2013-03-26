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
package org.apache.hcatalog.cli;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hcatalog.mapreduce.HCatInputStorageDriver;
import org.apache.hcatalog.mapreduce.HCatOutputStorageDriver;
import org.apache.hcatalog.storagehandler.HCatStorageHandler;

class DummyStorageHandler extends HCatStorageHandler {

    @Override
    public Configuration getConf() {
        return null;
    }

    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public void configureTableJobProperties(TableDesc arg0,
            Map<String, String> arg1) {
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return this;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return ColumnarSerDe.class;
    }

    @Override
    public void preCreateTable(Table table) throws MetaException {
    }

    @Override
    public void rollbackCreateTable(Table table) throws MetaException {
    }

    @Override
    public void commitCreateTable(Table table) throws MetaException {
    }

    @Override
    public void preDropTable(Table table) throws MetaException {
    }

    @Override
    public void rollbackDropTable(Table table) throws MetaException {

    }

    @Override
    public void commitDropTable(Table table, boolean deleteData)
            throws MetaException {
    }

    @Override
    public Class<? extends HCatInputStorageDriver> getInputStorageDriver() {
        return HCatInputStorageDriver.class;
    }

    @Override
    public Class<? extends HCatOutputStorageDriver> getOutputStorageDriver() {
        return HCatOutputStorageDriver.class;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        return new DummyAuthProvider();
    }

    private class DummyAuthProvider implements HiveAuthorizationProvider {

        @Override
        public Configuration getConf() {
            return null;
        }

        /* @param conf
         * @see org.apache.hadoop.conf.Configurable#setConf(org.apache.hadoop.conf.Configuration)
         */
        @Override
        public void setConf(Configuration conf) {
        }

        /* @param conf
        /* @throws HiveException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#init(org.apache.hadoop.conf.Configuration)
         */
        @Override
        public void init(Configuration conf) throws HiveException {
        }

        /* @return HiveAuthenticationProvider
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#getAuthenticator()
         */
        @Override
        public HiveAuthenticationProvider getAuthenticator() {
            return null;
        }

        /* @param authenticator
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#setAuthenticator(org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider)
         */
        @Override
        public void setAuthenticator(HiveAuthenticationProvider authenticator) {
        }

        /* @param readRequiredPriv
        /* @param writeRequiredPriv
        /* @throws HiveException
        /* @throws AuthorizationException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
         */
        @Override
        public void authorize(Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
                AuthorizationException {
        }

        /* @param db
        /* @param readRequiredPriv
        /* @param writeRequiredPriv
        /* @throws HiveException
        /* @throws AuthorizationException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.metastore.api.Database, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
         */
        @Override
        public void authorize(Database db, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
                AuthorizationException {
        }

        /* @param table
        /* @param readRequiredPriv
        /* @param writeRequiredPriv
        /* @throws HiveException
        /* @throws AuthorizationException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Table, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
         */
        @Override
        public void authorize(org.apache.hadoop.hive.ql.metadata.Table table, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
                AuthorizationException {
        }

        /* @param part
        /* @param readRequiredPriv
        /* @param writeRequiredPriv
        /* @throws HiveException
        /* @throws AuthorizationException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Partition, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
         */
        @Override
        public void authorize(Partition part, Privilege[] readRequiredPriv,
                Privilege[] writeRequiredPriv) throws HiveException,
                AuthorizationException {
        }

        /* @param table
        /* @param part
        /* @param columns
        /* @param readRequiredPriv
        /* @param writeRequiredPriv
        /* @throws HiveException
        /* @throws AuthorizationException
         * @see org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider#authorize(org.apache.hadoop.hive.ql.metadata.Table, org.apache.hadoop.hive.ql.metadata.Partition, java.util.List, org.apache.hadoop.hive.ql.security.authorization.Privilege[], org.apache.hadoop.hive.ql.security.authorization.Privilege[])
         */
        @Override
        public void authorize(org.apache.hadoop.hive.ql.metadata.Table table, Partition part, List<String> columns,
                Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
                throws HiveException, AuthorizationException {
        }

    }

}


