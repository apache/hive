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

package org.apache.hadoop.hive.registry.storage.core.impl.jdbc.config;

import com.zaxxer.hikari.HikariConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Factory to build simple HikariCP configurations */
public class HikariBasicConfig {
    // Hikari config to connect to MySql databases
    public static Map<String, Object> getMySqlHikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        config.put("dataSource.url", "jdbc:mysql://localhost/test");
        config.put("dataSource.user", "root");
        return config;
    }
    public static HikariConfig getMySqlHikariTestConfig() {
        final Map<String, Object> config = getMySqlHikariConfig();
        return getHikariConfig(config, false);
    }

    private static HikariConfig getHikariConfig(final Map<String, Object> config, boolean autoCommit) {
        HikariConfig hikariConfig = new HikariConfig(new Properties(){{putAll(config);}});
        // need to do this because of a bug in Hikari that does not allow the override of the
        // property dataSource.autoCommit
        hikariConfig.setAutoCommit(autoCommit);
        return hikariConfig;
    }


    // Hikari config to connect to H2 databases. Useful for integration tests
    public static Map<String, Object> getH2HikariConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("dataSourceClassName", "org.h2.jdbcx.JdbcDataSource");
//        In memory configuration. Faster, useful for integration tests
        config.put("dataSource.URL", "jdbc:h2:mem:test;MODE=MySQL;DATABASE_TO_UPPER=false");
//        Embedded configuration. Facilitates debugging by allowing connecting to DB and querying tables
//        config.put("dataSource.URL", "jdbc:h2:~/test;MODE=MySQL;DATABASE_TO_UPPER=false");
        return config;
    }

    public static HikariConfig getH2HikariTestConfig() {
        Map<String, Object> config = getH2HikariConfig();
        return getHikariConfig(config, false);
    }
}