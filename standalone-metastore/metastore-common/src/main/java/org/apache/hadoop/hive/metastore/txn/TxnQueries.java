/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn;

public class TxnQueries {
    public static final String SHOW_COMPACTION_ORDERBY_CLAUSE =
            " ORDER BY CASE " +
                    "   WHEN \"CC_END\" > \"CC_START\" and \"CC_END\" > \"CC_COMMIT_TIME\" " +
                    "     THEN \"CC_END\" " +
                    "   WHEN \"CC_START\" > \"CC_COMMIT_TIME\" " +
                    "     THEN \"CC_START\" " +
                    "   ELSE \"CC_COMMIT_TIME\" " +
                    " END desc ," +
                    " \"CC_ENQUEUE_TIME\" asc";

    public static final String SHOW_COMPACTION_QUERY =
            "SELECT XX.* FROM ( SELECT " +
                    "  \"CQ_DATABASE\" AS \"CC_DATABASE\", \"CQ_TABLE\" AS \"CC_TABLE\", \"CQ_PARTITION\" AS \"CC_PARTITION\", " +
                    "  \"CQ_STATE\" AS \"CC_STATE\", \"CQ_TYPE\" AS \"CC_TYPE\", \"CQ_WORKER_ID\" AS \"CC_WORKER_ID\", " +
                    "  \"CQ_START\" AS \"CC_START\", -1 \"CC_END\", \"CQ_RUN_AS\" AS \"CC_RUN_AS\", " +
                    "  \"CQ_HADOOP_JOB_ID\" AS \"CC_HADOOP_JOB_ID\", \"CQ_ID\" AS \"CC_ID\", \"CQ_ERROR_MESSAGE\" AS \"CC_ERROR_MESSAGE\", " +
                    "  \"CQ_ENQUEUE_TIME\" AS \"CC_ENQUEUE_TIME\", \"CQ_WORKER_VERSION\" AS \"CC_WORKER_VERSION\", " +
                    "  \"CQ_INITIATOR_ID\" AS \"CC_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\" AS \"CC_INITIATOR_VERSION\", " +
                    "  \"CQ_CLEANER_START\" AS \"CC_CLEANER_START\", \"CQ_POOL_NAME\" AS \"CC_POOL_NAME\", \"CQ_TXN_ID\" AS \"CC_TXN_ID\", " +
                    "  \"CQ_NEXT_TXN_ID\" AS \"CC_NEXT_TXN_ID\", \"CQ_COMMIT_TIME\" AS \"CC_COMMIT_TIME\", " +
                    "  \"CQ_HIGHEST_WRITE_ID\" AS \"CC_HIGHEST_WRITE_ID\" " +
                    "FROM " +
                    "  \"COMPACTION_QUEUE\" " +
                    "UNION ALL " +
                    "SELECT " +
                    "  \"CC_DATABASE\" , \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_WORKER_ID\", " +
                    "  \"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HADOOP_JOB_ID\", \"CC_ID\", \"CC_ERROR_MESSAGE\", " +
                    "  \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", " +
                    "   -1 , \"CC_POOL_NAME\", \"CC_TXN_ID\", \"CC_NEXT_TXN_ID\", \"CC_COMMIT_TIME\", " +
                    "  \"CC_HIGHEST_WRITE_ID\"" +
                    "FROM " +
                    "  \"COMPLETED_COMPACTIONS\" ) XX ";


    public static final String SELECT_COMPACTION_QUEUE_BY_COMPID =
            "SELECT XX.* FROM ( SELECT " +
                    "   \"CQ_ID\" AS \"CC_ID\", \"CQ_DATABASE\" AS \"CC_DATABASE\", \"CQ_TABLE\" AS \"CC_TABLE\", \"CQ_PARTITION\" AS \"CC_PARTITION\", " +
                    "   \"CQ_STATE\" AS \"CC_STATE\", \"CQ_TYPE\" AS \"CC_TYPE\", \"CQ_TBLPROPERTIES\" AS \"CC_TBLPROPERTIES\", \"CQ_WORKER_ID\" AS \"CC_WORKER_ID\", " +
                    "   \"CQ_START\" AS \"CC_START\", \"CQ_RUN_AS\" AS \"CC_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\" AS \"CC_HIGHEST_WRITE_ID\", \"CQ_META_INFO\" AS \"CC_META_INFO\"," +
                    "   \"CQ_HADOOP_JOB_ID\" AS \"CC_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\" AS \"CC_ERROR_MESSAGE\",  \"CQ_ENQUEUE_TIME\" AS \"CC_ENQUEUE_TIME\"," +
                    "   \"CQ_WORKER_VERSION\" AS \"CC_WORKER_VERSION\", \"CQ_INITIATOR_ID\" AS \"CC_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\" AS \"CC_INITIATOR_VERSION\", " +
                    "   \"CQ_RETRY_RETENTION\" AS \"CC_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\" AS \"CC_NEXT_TXN_ID\", \"CQ_TXN_ID\" AS \"CC_TXN_ID\", " +
                    "   \"CQ_COMMIT_TIME\" AS \"CC_COMMIT_TIME\", \"CQ_POOL_NAME\" AS \"CC_POOL_NAME\",  " +
                    "   \"CQ_NUMBER_OF_BUCKETS\" AS \"CC_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" AS \"CC_ORDER_BY\" " +
                    "   FROM " +
                    "   \"COMPACTION_QUEUE\" " +
                    "   UNION ALL " +
                    "   SELECT " +
                    "   \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", " +
                    "   \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", \"CC_START\", \"CC_RUN_AS\", " +
                    "   \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", \"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", " +
                    "   \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", " +
                    "    -1 , \"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_NEXT_TXN_ID\", \"CC_POOL_NAME\", " +
                    "   \"CC_NUMBER_OF_BUCKETS\", \"CC_ORDER_BY\" " +
                    "   FROM   " +
                    "   \"COMPLETED_COMPACTIONS\") XX ";


    public static final String INSERT_INTO_COMPLETED_COMPACTION =
            "INSERT INTO \"COMPLETED_COMPACTIONS\" " +
                    "   (\"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", " +
                    "   \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", \"CC_START\", \"CC_END\", \"CC_RUN_AS\", " +
                    "   \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", \"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", " +
                    "   \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\"," +
                    "   \"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_COMMIT_TIME\", \"CC_POOL_NAME\", \"CC_NUMBER_OF_BUCKETS\", " +
                    "   \"CC_ORDER_BY\") " +
                    "   VALUES(?,?,?,?,?, ?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
}
