/**
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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * Interface to format table and index information.  We can format it
 * for human readability (lines of text) or for machine readability
 * (json).
 */
public interface MetaDataFormatter {
    /**
     * Generic error code.  This and the other error codes are
     * designed to match the HTTP status codes.
     */
    static final int ERROR = 500;

    /**
     * Missing error code.
     */
    static final int MISSING = 404;

    /**
     * Conflict error code.
     */
    static final int CONFLICT = 409;

    /**
     * Write an error message.
     */
    public void error(OutputStream out, String msg, int errorCode)
        throws HiveException;

    /**
     * Write a log warn message.
     */
    public void logWarn(OutputStream out, String msg, int errorCode)
        throws HiveException;

    /**
     * Write a log info message.
     */
    public void logInfo(OutputStream out, String msg, int errorCode)
        throws HiveException;

    /**
     * Write a console error message.
     */
    public void consoleError(LogHelper console, String msg, int errorCode);

    /**
     * Write a console error message.
     */
    public void consoleError(LogHelper console, String msg, String detail,
                             int errorCode);

    /**
     * Show a list of tables.
     */
    public void showTables(DataOutputStream out, Set<String> tables)
        throws HiveException;

    /**
     * Describe table.
     */
    public void describeTable(DataOutputStream out,
                              String colPath, String tableName,
                              Table tbl, Partition part, List<FieldSchema> cols,
                              boolean isFormatted, boolean isExt)
        throws HiveException;

   /**
     * Show the table status.
     */
    public void showTableStatus(DataOutputStream out,
                                Hive db,
                                HiveConf conf,
                                List<Table> tbls,
                                Map<String, String> part,
                                Partition par)
        throws HiveException;

    /**
     * Show the table partitions.
     */
    public void showTablePartitons(DataOutputStream out,
                                   List<String> parts)
        throws HiveException;

    /**
     * Show the databases
     */
    public void showDatabases(DataOutputStream out, List<String> databases)
        throws HiveException;

    /**
     * Describe a database.
     */
    public void showDatabaseDescription(DataOutputStream out,
                                        String database,
                                        String comment,
                                        String location,
                                        Map<String, String> params)
        throws HiveException;
}

