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

package org.apache.hadoop.hive.ql;

/**
 * QTestExternalDB composite used as information holder for creating external databases of different types.
 */
public final class QTestExternalDB {

    private String externalDBType;
    private String externalDBInitScript;
    private String externalDBCleanupScript;

    public QTestExternalDB() { }

    public static QTestExternalDB createDefaultExtDB(String externalDBType) {

        QTestExternalDB externalDB = new QTestExternalDB();

        externalDB.setExternalDBType(externalDBType);
        externalDB.setExternalDBInitScript(String.format("q_test_extDB_init.%s.sql", externalDBType));
        externalDB.setExternalDBCleanupScript(String.format("q_test_extDB_cleanup.%s.sql", externalDBType));

        return externalDB;
    }

    private void setExternalDBType(String dbType) { this.externalDBType = dbType; }

    public String getExternalDBType() { return externalDBType; }

    private void setExternalDBInitScript(String InitScript) { this.externalDBInitScript = InitScript; }

    public String getExternalDBInitScript() { return externalDBInitScript; }

    private void setExternalDBCleanupScript(String cleanupScript) { this.externalDBCleanupScript = cleanupScript; }

    public String getExternalDBCleanupScript() { return externalDBCleanupScript; }

}
