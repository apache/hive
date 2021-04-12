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

package org.apache.hadoop.hive.ql.ddl.table.create.show;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableOperation;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hive.common.util.HiveStringUtils;
import org.stringtemplate.v4.ST;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Operation process showing the creation of a table.
 */
public class ShowCreateTableOperation extends DDLOperation<ShowCreateTableDesc> {
  private static final String EXTERNAL = "external";
  private static final String TEMPORARY = "temporary";
  private static final String DATABASE_NAME = "databaseName";
  private static final String TABLE_NAME = "tableName";
  private static final String LIST_COLUMNS = "columns";
  private static final String COMMENT = "comment";
  private static final String PARTITIONS = "partitions";
  private static final String BUCKETS = "buckets";
  private static final String SKEWED = "skewedinfo";
  private static final String ROW_FORMAT = "row_format";
  private static final String LOCATION_BLOCK = "location_block";
  private static final String LOCATION = "location";
  private static final String PROPERTIES = "properties";

  public ShowCreateTableOperation(DDLOperationContext context, ShowCreateTableDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    // get the create table statement for the table and populate the output
    try (DataOutputStream outStream = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      Table table = context.getDb().getTable(desc.getDatabaseName(), desc.getTableName());
      DDLPlanUtils ddlObj = new DDLPlanUtils();
      String command = table.isView() ? ddlObj.getCreateViewCommand(table, desc.isRelative())
          : ddlObj.getCreateTableCommand(table, desc.isRelative());

      outStream.write(command.getBytes(StandardCharsets.UTF_8));
      return 0;
    } catch (IOException e) {
      LOG.info("Show create table failed", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }
}
