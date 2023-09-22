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

package org.apache.hadoop.hive.metastore;

import com.facebook.fb303.fb_status;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * This abstract class can be extended by any remote cache that implements HMS APIs.
 * The idea behind introducing this abstract class is not to break the build of remote cache,
 * whenever we add new HMS APIs.
 */
public abstract class AbstractThriftHiveMetastore implements Iface {

    @Override
    public String getMetaConf(String key) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void setMetaConf(String key, String value) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_catalog(CreateCatalogRequest catalog)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_catalog(AlterCatalogRequest rqst)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetCatalogResponse get_catalog(GetCatalogRequest catName)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetCatalogsResponse get_catalogs() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_catalog(DropCatalogRequest catName)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_database(Database database)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Database get_database_req(GetDatabaseRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_database(String name, boolean deleteData, boolean cascade)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_databases(String pattern) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_all_databases() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_database(String dbname, Database db)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_dataconnector(DataConnector connector)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public DataConnector get_dataconnector_req(GetDataConnectorRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_dataconnector(String name, boolean ifNotExists, boolean checkReferences)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_dataconnectors() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_dataconnector(String name, DataConnector connector)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Type get_type(String name) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean create_type(Type type)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_type(String type) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Map<String, Type> get_type_all(String name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<FieldSchema> get_fields(String db_name, String table_name)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<FieldSchema> get_fields_with_environment_context(String db_name, String table_name,
                                                                 EnvironmentContext environment_context)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetFieldsResponse get_fields_req(GetFieldsRequest req)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<FieldSchema> get_schema(String db_name, String table_name)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<FieldSchema> get_schema_with_environment_context(String db_name, String table_name,
                                                                 EnvironmentContext environment_context)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetSchemaResponse get_schema_req(GetSchemaRequest req)
            throws MetaException, UnknownTableException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_table(Table tbl)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
                                              List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
                                              List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
                                              List<SQLCheckConstraint> checkConstraints)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_table_req(CreateTableRequest request)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_constraint(DropConstraintRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_primary_key(AddPrimaryKeyRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_foreign_key(AddForeignKeyRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_unique_constraint(AddUniqueConstraintRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_not_null_constraint(AddNotNullConstraintRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_default_constraint(AddDefaultConstraintRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_check_constraint(AddCheckConstraintRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_table(String dbname, String name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_table_with_environment_context(String dbname, String name, boolean deleteData,
                                                    EnvironmentContext environment_context) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void truncate_table(String dbName, String tableName, List<String> partNames)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public TruncateTableResponse truncate_table_req(TruncateTableRequest req) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_tables(String db_name, String pattern) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_tables_by_type(String db_name, String pattern, String tableType)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Table> get_all_materialized_view_objects_for_rewriting() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_materialized_views_for_rewriting(String db_name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<TableMeta> get_table_meta(String db_patterns, String tbl_patterns, List<String> tbl_types)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_all_tables(String db_name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Table get_table(String dbname, String tbl_name)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<ExtendedTableInfo> get_tables_ext(GetTablesExtRequest req) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetTableResult get_table_req(GetTableRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Materialization get_materialization_invalidation_info(CreationMetadata creation_metadata, String validTxnList)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void update_creation_metadata(String catName, String dbname, String tbl_name,
                                         CreationMetadata creation_metadata)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
            throws MetaException, InvalidOperationException, UnknownDBException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_table(String dbname, String tbl_name, Table new_tbl)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_table_with_environment_context(String dbname, String tbl_name, Table new_tbl,
                                                     EnvironmentContext environment_context) throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AlterTableResponse alter_table_req(AlterTableRequest req)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition add_partition(Partition new_part)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition add_partition_with_environment_context(Partition new_part,
                                                            EnvironmentContext environment_context)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public int add_partitions(List<Partition> new_parts)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public int add_partitions_pspec(List<PartitionSpec> new_parts)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition append_partition_with_environment_context(String db_name, String tbl_name,
                                                               List<String> part_vals, EnvironmentContext environment_context)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition append_partition_by_name(String db_name, String tbl_name, String part_name)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition append_partition_by_name_with_environment_context(String db_name, String tbl_name,
                                                                       String part_name, EnvironmentContext environment_context)
            throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_partition_with_environment_context(String db_name, String tbl_name,
                                                           List<String> part_vals, boolean deleteData, EnvironmentContext environment_context)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_partition_by_name_with_environment_context(String db_name, String tbl_name,
                                                                   String part_name, boolean deleteData, EnvironmentContext environment_context)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPartitionResponse get_partition_req(GetPartitionRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition exchange_partition(Map<String, String> partitionSpecs, String source_db,
                                        String source_table_name, String dest_db, String dest_table_name)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String source_db,
                                               String source_table_name, String dest_db, String dest_table_name)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition get_partition_with_auth(String db_name, String tbl_name, List<String> part_vals,
                                             String user_name, List<String> group_names) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PartitionsResponse get_partitions_req(PartitionsRequest req)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_with_auth(String db_name, String tbl_name, short max_parts,
                                                    String user_name, List<String> group_names) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PartitionValuesResponse get_partition_values(PartitionValuesRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals,
                                             short max_parts) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_ps_with_auth(String db_name, String tbl_name, List<String> part_vals,
                                                       short max_parts, String user_name, List<String> group_names)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPartitionsPsWithAuthResponse get_partitions_ps_with_auth_req(GetPartitionsPsWithAuthRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals,
                                               short max_parts) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPartitionNamesPsResponse get_partition_names_ps_req(GetPartitionNamesPsRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_partition_names_req(PartitionsByExprRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter,
                                                    short max_parts) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter,
                                                        int max_parts) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PartitionsSpecByExprResult get_partitions_spec_by_expr(PartitionsByExprRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public int get_num_partitions_by_filter(String db_name, String tbl_name, String filter)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPartitionsByNamesResult get_partitions_by_names_req(GetPartitionsByNamesRequest req)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Partition> get_partitions_by_filter_req(GetPartitionsByFilterRequest req) throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_partition(String db_name, String tbl_name, Partition new_part)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_partitions_with_environment_context(String db_name, String tbl_name,
                                                          List<Partition> new_parts, EnvironmentContext environment_context)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AlterPartitionsResponse alter_partitions_req(AlterPartitionsRequest req)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_partition_with_environment_context(String db_name, String tbl_name, Partition new_part,
                                                         EnvironmentContext environment_context) throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public RenamePartitionResponse rename_partition_req(RenamePartitionRequest req)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String get_config_value(String name, String defaultValue)
            throws ConfigValSecurityException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Map<String, String> partition_name_to_spec(String part_name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> part_vals,
                                      PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
            InvalidPartitionException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> part_vals,
                                             PartitionEventType eventType)
            throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
            InvalidPartitionException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AllTableConstraintsResponse get_all_table_constraints(AllTableConstraintsRequest request)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean update_table_column_statistics(ColumnStatistics stats_obj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean update_partition_column_statistics(ColumnStatistics stats_obj)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public SetPartitionsStatsResponse update_table_column_statistics_req(SetPartitionsStatsRequest req)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public SetPartitionsStatsResponse update_partition_column_statistics_req(SetPartitionsStatsRequest req)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
            throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ColumnStatistics get_partition_column_statistics(String db_name, String tbl_name, String part_name,
                                                            String col_name)
            throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public TableStatsResult get_table_statistics_req(TableStatsRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AggrStats get_aggr_stats_for(PartitionsStatsRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean set_aggr_stats_for(SetPartitionsStatsRequest request)
            throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name,
                                                      String col_name, String engine)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name,
                                                  String engine)
            throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_function(Function func)
            throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_function(String dbName, String funcName)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_function(String dbName, String funcName, Function newFunc)
            throws InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_functions(String dbName, String pattern) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Function get_function(String dbName, String funcName)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetAllFunctionsResponse get_all_functions() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean create_role(Role role) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean drop_role(String role_name) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_role_names() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean grant_role(String role_name, String principal_name, PrincipalType principal_type,
                              String grantor, PrincipalType grantorType, boolean grant_option) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<Role> list_roles(String principal_name, PrincipalType principal_type)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
            GetRoleGrantsForPrincipalRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name,
                                                   List<String> group_names) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
                                                     HiveObjectRef hiveObject) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
                                                           GrantRevokePrivilegeRequest grantRequest) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> set_ugi(String user_name, List<String> group_names) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public long renew_delegation_token(String token_str_form) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void cancel_delegation_token(String token_str_form) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean add_token(String token_identifier, String delegation_token) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean remove_token(String token_identifier) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String get_token(String token_identifier) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_all_token_identifiers() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public int add_master_key(String key) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void update_master_key(int seq_number, String key)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean remove_master_key(int key_seq) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_master_keys() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetOpenTxnsResponse get_open_txns() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void abort_txn(AbortTxnRequest rqst) throws NoSuchTxnException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void abort_txns(AbortTxnsRequest rqst) throws NoSuchTxnException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void commit_txn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public long get_latest_txnid_in_conflict(long txnId) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest rqst)
            throws NoSuchTxnException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public AllocateTableWriteIdsResponse allocate_table_write_ids(AllocateTableWriteIdsRequest rqst)
            throws NoSuchTxnException, TxnAbortedException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public MaxAllocatedTableWriteIdResponse get_max_allocated_table_write_id(
            MaxAllocatedTableWriteIdRequest rqst) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void seed_write_id(SeedTableWriteIdsRequest rqst) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void seed_txn_id(SeedTxnIdRequest rqst) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public LockResponse check_lock(CheckLockRequest rqst)
            throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void heartbeat(HeartbeatRequest ids)
            throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void compact(CompactionRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public CompactionResponse compact2(CompactionRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_dynamic_partitions(AddDynamicPartitions rqst)
            throws NoSuchTxnException, TxnAbortedException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Deprecated
    @Override
    public OptionalCompactionInfoStruct find_next_compact(String workerId) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public OptionalCompactionInfoStruct find_next_compact2(FindNextCompactRequest rqst)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void update_compactor_state(CompactionInfoStruct cr, long txn_id) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> find_columns_with_stats(CompactionInfoStruct cr) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void mark_cleaned(CompactionInfoStruct cr) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void mark_compacted(CompactionInfoStruct cr) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void mark_failed(CompactionInfoStruct cr) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void set_hadoop_jobid(String jobId, long cq_id) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetLatestCommittedCompactionInfoResponse get_latest_committed_compaction_info(
            GetLatestCommittedCompactionInfoRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public CurrentNotificationEventId get_current_notificationEventId() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest rqst)
            throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void flushCache() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WriteNotificationLogResponse add_write_notification_log(WriteNotificationLogRequest rqst)
            throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public CmRecycleResponse cm_recycle(CmRecycleRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req)
            throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String get_metastore_db_uuid() throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest request)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMGetActiveResourcePlanResponse get_active_resource_plan(WMGetActiveResourcePlanRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest request)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest request)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest request)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest request)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest request)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest request)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
            WMGetTriggersForResourePlanRequest request) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest request)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest request)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest request)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(WMCreateOrUpdateMappingRequest request)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest request)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
            WMCreateOrDropTriggerToPoolMappingRequest request)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_ischema(ISchema schema)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void alter_ischema(AlterISchemaRequest rqst)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ISchema get_ischema(ISchemaName name) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_ischema(ISchemaName name)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_schema_version(SchemaVersion schemaVersion)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersion)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public SchemaVersion get_schema_latest_version(ISchemaName schemaName)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<SchemaVersion> get_schema_all_versions(ISchemaName schemaName)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_schema_version(SchemaVersionDescriptor schemaVersion)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst rqst)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest rqst)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void set_schema_version_state(SetSchemaVersionStateRequest rqst)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_serde(SerDeInfo serde) throws AlreadyExistsException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public SerDeInfo get_serde(GetSerdeRequest rqst) throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId)
            throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId)
            throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_runtime_stats(RuntimeStat stat) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest rqst) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetPartitionsResponse get_partitions_with_specs(GetPartitionsRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ScheduledQueryPollResponse scheduled_query_poll(ScheduledQueryPollRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void scheduled_query_maintenance(ScheduledQueryMaintenanceRequest request)
            throws MetaException, NoSuchObjectException, AlreadyExistsException, InvalidInputException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void scheduled_query_progress(ScheduledQueryProgressInfo info)
            throws MetaException, InvalidOperationException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ScheduledQuery get_scheduled_query(ScheduledQueryKey scheduleKey)
            throws MetaException, NoSuchObjectException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_replication_metrics(ReplicationMetricList replicationMetricList)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public ReplicationMetricList get_replication_metrics(GetReplicationMetricsRequest rqst)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public GetOpenTxnsResponse get_open_txns_req(GetOpenTxnsRequest getOpenTxnsRequest) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void create_stored_procedure(StoredProcedure proc)
            throws NoSuchObjectException, MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public StoredProcedure get_stored_procedure(StoredProcedureRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_stored_procedure(StoredProcedureRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_all_stored_procedures(ListStoredProcedureRequest request)
            throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Package find_package(GetPackageRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void add_package(AddPackageRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<String> get_all_packages(ListPackageRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void drop_package(DropPackageRequest request) throws MetaException, TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String getName() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String getVersion() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public fb_status getStatus() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String getStatusDetails() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Map<String, Long> getCounters() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public long getCounter(String s) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void setOption(String s, String s1) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String getOption(String s) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public Map<String, String> getOptions() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public String getCpuProfile(int i) throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public long aliveSince() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void reinitialize() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public void shutdown() throws TException {
        throw new UnsupportedOperationException("this method is not supported");
    }

    @Override
    public List<WriteEventInfo> get_all_write_event_info(GetAllWriteEventInfoRequest request)
        throws MetaException, org.apache.thrift.TException {
        throw new UnsupportedOperationException("this method is not supported");
    }
}
