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
package org.apache.hadoop.hive.ql.queryhistory.schema;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class Schema {
  public static final int CURRENT_VERSION = 1;

  // how many bytes are consumed by the "short" fields in a record
  // rough estimate (once per JVM), which is much efficient than counting all the fields
  public static final long BASE_RECORD_SIZE_IN_MEMORY_BYTES = calculateSimpleFieldsSize();

  private static int calculateSimpleFieldsSize() {
    int baseSize = 0;
    for (Schema.Field field : Schema.Field.values()) {
      switch (field.getType()) {
        case "string":
          // consider an average 20 chars length
          // longer fields are going to be considered again
          baseSize += 20 * Character.BYTES;
          break;
        case "int":
          baseSize += 4;
          break;
        case "bigint":
          // stored as long
          baseSize += 8;
          break;
        case "timestamp":
          baseSize += 16;
          break;
      }
    }
    return baseSize;
  }

  interface SchemaField {
    String getName();
    String getType();
    String getDescription();
    boolean isPartitioningCol();
  }

  public static ObjectInspector INSPECTOR = null;

  static {
    INSPECTOR = createInspector(Field.values());
  }

  private final ArrayList<FieldSchema> fields;

  public Schema() {
    this.fields = new ArrayList<>();

    Arrays.asList(Field.values())
        .forEach(field -> this.fields.add(new FieldSchema(field.name, field.type, field.description)));
  }

  @VisibleForTesting
  static ObjectInspector createInspector(SchemaField[] fields) {
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        Arrays.stream(fields).map(SchemaField::getName).collect(Collectors.toList()),
        Arrays.stream(fields).map(field -> PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
            TypeInfoFactory.getPrimitiveTypeInfo(field.getType()))).collect(Collectors.toList()));
  }

  public List<FieldSchema> getFields() {
    return fields;
  }


  public List<? extends FieldSchema> getPartCols() {
    return Arrays.stream(Schema.Field.values()).filter(Schema.Field::isPartitioningCol)
        .map(field -> new FieldSchema(field.getName(), field.getType(),
            field.getDescription())).collect(Collectors.toList());
  }

  public enum Field implements SchemaField {
    // 1. BASIC FIELDS
    QUERY_HISTORY_SCHEMA_VERSION("query_history_schema_version", "int",
        "Query history schema version when this record was written"),
    HIVE_VERSION("hive_version", "string", "Hive version in HS2 when this record was written"),
    QUERY_ID("query_id", "string", "Hive assigned query identifier"),
    SESSION_ID("session_id", "string", "Hive assigned session identifier"),
    OPERATION_ID("operation_id", "string", "Hive assigned operation identifier"),
    EXECUTION_ENGINE("execution_engine", "string",
        "Execution engine (mostly 'tez', or older ones in case they're not disabled yet)"),
    EXECUTION_MODE("execution_mode", "string",
        "Whether the query runs in llap or tez container mode"),
    TEZ_DAG_ID("tez_dag_id", "string", "Tez DAG id in which the query ran, if any"),
    TEZ_APP_ID("tez_app_id", "string", "Tez application id in which the query ran if any"),
    TEZ_SESSION_ID("tez_session_id", "string", "Tez session id in which the query ran if any"),
    CLUSTER_ID("cluster_id", "string",
        "Use specified string to uniquely identify an instance", true),
    SQL("sql", "string", "The SQL query string submitted by the user"),
    SESSION_TYPE("session_type", "string",
        "The type of session (HIVESERVER2, OTHER)"),
    HIVERSERVER2_PROTOCOL_VERSION("hiveserver2_protocol_version", "int",
        "The protocol used by the client as defined in TCLIService, used in HiveConnection"),
    CLUSTER_USER("cluster_user", "string",
        "Effective user of the query on the cluster (this is most probably 'hive' if doAs is disabled)"),
    END_USER("end_user", "string", "Username from an authenticated client"),
    DB_NAME("db_name", "string", "The current database while running the query"),
    TEZ_COORDINATOR("tez_coordinator", "string",
        "Address (host:port) of the tez coordinator for the query"),
    QUERY_STATE("query_state", "string", "The state of the query as an OperationState value"),
    QUERY_TYPE("query_type", "string",
        "The query type according to the semantic analyzer: DQL, DDL, DML, DCL, STATS, empty otherwise"),
    OPERATION("operation", "string",
        "The hive operation name derived from syntax token or custom semantic analysis logic."),
    SERVER_ADDRESS("server_address", "string",
        "The address of HS2 to which the client connected"),
    SERVER_PORT("server_port", "int", "The TCP port of HS2 to which the client connected"),
    CLIENT_ADDRESS("client_address", "string", "The IP address from where the client connected"),
    START_TIME_UTC("start_time_utc", "timestamp", "The UTC timestamp when the query started"),
    END_TIME_UTC("end_time_utc", "timestamp", "The UTC timestamp when the query finished"),
    START_TIME("start_time", "timestamp",
        "A timestamp (in the the server's timezone) when the query started"),
    END_TIME("end_time", "timestamp",
        "A timestamp (in the the server's timezone) when the query finished"),
    TOTAL_TIME_MS("total_time_ms", "bigint",
        "Difference between the end time and start time (ms)"),
    PLANNING_DURATION("planning_duration", "bigint",
        "Duration of the compile/planning phase (ms)"),
    PLANNING_START_TIME("planning_start_time", "timestamp",
        "Timestamp when the compile/planning phase started"),
    PREPARE_PLAN_DURATION("prepare_plan_duration", "bigint",
        "Duration of preparing the DAG to run (ms)"),
    PREPARE_PLAN_START_TIME("prepare_plan_start_time", "timestamp",
        "Timestamp when the prepare plan phase started"),
    GET_SESSION_DURATION("get_session_duration", "bigint",
        "Duration of getting the Tez session (ms)"),
    GET_SESSION_START_TIME("get_session_start_time", "timestamp",
        "Timestamp when the getting session phase started"),
    EXECUTION_DURATION("execution_duration", "bigint", "Duration of the DAG execution phase (ms)"),
    EXECUTION_START_TIME("execution_start_time", "timestamp",
        "Timestamp when the DAG execution phase started"),
    FAILURE_REASON("failure_reason", "string",
        "A captured error message as a failure reason if any"),
    NUM_ROWS_FETCHED("num_rows_fetched", "int", "Number of rows fetched by the query"),
    PLAN("plan", "string", "Full text of the query plan"),
    USED_TABLES("used_tables", "string",
        "Comma-separated list of the tables used by this query"),
    EXEC_SUMMARY("exec_summary", "string", "Full text of the exec summary"),
    CONFIGURATION_OPTIONS_CHANGED("configuration_options_changed", "string",
        "The changed configuration options in the session of the query"),

    // 2. RUNTIME COUNTERS
    // most of them are not applicable for HS2-only queries (e.g. fetch optimization), in which case a default -1 is
    // used
    // the exact meaning of the counters are not documented here in detail, as the query history service project
    // itself is not responsible for documenting, maintaining the counters, by the time of the initial
    // implementation, every counter was added that made some sense to be queried later

    // 2a. COUNTERS APPLICABLE TO TEZ CONTAINER MODE + LLAP
    // counter group: org.apache.tez.common.counters.DAGCounter
    // Counter: TOTAL_LAUNCHED_TASKS
    TOTAL_LAUNCHED_TASKS("total_tasks", "int",
        "Total number of tez tasks that were started to execute the query"),
    // Counter: NUM_SUCCEEDED_TASKS
    NUM_SUCCEEDED_TASKS("succeeded_tasks", "int","Number of successful tasks"),
    // Counter: NUM_KILLED_TASKS
    NUM_KILLED_TASKS("killed_tasks", "int","Number of killed tasks"),
    // Counter: NUM_FAILED_TASKS
    NUM_FAILED_TASKS("failed_tasks", "int","Number of failed task attempts"),
    // Counter: WALL_CLOCK_MILLIS
    TASK_DURATION_MILLIS("task_duration_millis", "bigint",
        "Total tez task duration in milliseconds"),
    // Counter: NODE_USED_COUNT
    NODE_USED_COUNT("node_used_count", "int", "Number of nodes used to run this query"),
    // Counter: NODE_TOTAL_COUNT
    NODE_TOTAL_COUNT("node_total_count", "int",
        "Total number of nodes visible when this query runs"),

    // counter group: org.apache.tez.common.counters.TaskCounter
    // all of this group are applicable to both Tez and LLAP
    REDUCE_INPUT_GROUPS("reduce_input_groups", "bigint", "Input groups seen by reducer tasks"),
    REDUCE_INPUT_RECORDS("reduce_input_records", "bigint",
        "Input records seen by reducer tasks"),
    SPILLED_RECORDS("spilled_records", "bigint", "Number of spilled records during shuffling"),
    NUM_SHUFFLED_INPUTS("num_shuffled_inputs", "bigint",
        "Number of physical inputs from which data was copied"),
    NUM_FAILED_SHUFFLE_INPUTS("num_failed_shuffle_inputs", "bigint",
        "Number of failed copy attempts of physical inputs"),
    INPUT_RECORDS_PROCESSED("input_records_processed", "bigint",
        "Number of input records that were actually processed"),
    INPUT_SPLIT_LENGTH_BYTES("input_split_length_bytes", "bigint",
        "Number of bytes seen as input splits"),
    OUTPUT_RECORDS("output_records", "bigint",
        "Output records seen from all vertices (intermediate and final outputs too)"),
    OUTPUT_BYTES_PHYSICAL("output_bytes_physical", "bigint",
        "Bytes actually written by outputs (serialized, compression considered)"),
    SHUFFLE_CHUNK_COUNT("shuffle_chunk_count", "bigint",
        "Number of shuffled files via shuffle handler"),
    SHUFFLE_BYTES("shuffle_bytes", "bigint",
        "Total number of shuffled bytes, including locally fetched (no http) files"),
    SHUFFLE_BYTES_DISK_DIRECT("shuffle_bytes_disk_direct", "bigint",
        "Bytes shuffled by direct disk access (local fetch optimization)"),
    SHUFFLE_PHASE_TIME("shuffle_phase_time", "bigint",
        "Time taken to shuffle data in milliseconds"),
    MERGE_PHASE_TIME("merge_phase_time", "bigint",
        "Time taken to merge data retrieved during shuffle in milliseconds");

    final String name;
    final String type;
    final String description;
    final boolean isPartitioningCol;

    Field(String name, String type, String description) {
      this(name, type, description, false);
    }

    Field(String name, String type, String description, boolean isPartitioningCol) {
      this.name = name;
      this.type = type;
      this.description = description;
      this.isPartitioningCol = isPartitioningCol;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String getType() {
      return type;
    }

    @Override
    public String getDescription() {
      return description;
    }

    @Override
    public boolean isPartitioningCol() {
      return isPartitioningCol;
    }
  }
}
