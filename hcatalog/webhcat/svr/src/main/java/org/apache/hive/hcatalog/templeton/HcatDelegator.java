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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;
import org.eclipse.jetty.http.HttpStatus;


/**
 * Run hcat on the local server using the ExecService.  This is
 * the backend of the ddl web service.
 */
public class HcatDelegator extends LauncherDelegator {
  private static final Logger LOG = LoggerFactory.getLogger(HcatDelegator.class);
  private ExecService execService;

  public HcatDelegator(AppConfig appConf, ExecService execService) {
    super(appConf);
    this.execService = execService;
  }

  /**
   * Run the local hcat executable.
   */
  public ExecBean run(String user, String exec, boolean format,
            String group, String permissions)
    throws NotAuthorizedException, BusyException, ExecuteException, IOException {
    SecureProxySupport proxy = new SecureProxySupport();
    try {
      List<String> args = makeArgs(exec, format, group, permissions);
      proxy.open(user, appConf);

      // Setup the hadoop vars to specify the user.
      String cp = makeOverrideClasspath(appConf);
      Map<String, String> env = TempletonUtils.hadoopUserEnv(user, cp);
      proxy.addEnv(env);
      proxy.addArgs(args);
      if (appConf.clusterHcat().toLowerCase().endsWith(".py")) {
        return execService.run(appConf.clusterPython(), args, env);
      } else {
        return execService.run(appConf.clusterHcat(), args, env);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    } finally {
      if (proxy != null)
        proxy.close();
    }
  }

  private List<String> makeArgs(String exec, boolean format,
                  String group, String permissions) {
    ArrayList<String> args = new ArrayList<String>();
    if (appConf.clusterHcat().toLowerCase().endsWith(".py")) {
      // hcat.py will become the first argument pass to command "python"
      args.add(appConf.clusterHcat());
    }
    args.add("-e");
    args.add('"' + exec + '"');
    if (TempletonUtils.isset(group)) {
      args.add("-g");
      args.add(group);
    }
    if (TempletonUtils.isset(permissions)) {
      args.add("-p");
      args.add(permissions);
    }
    if (format) {
      args.add("-D");
      args.add("hive.ddl.output.format=json");
      // Use both args to ease development.  Delete this one on
      // May 1.
      args.add("-D");
      args.add("hive.format=json");
    }
    LOG.info("Main.getAppConfigInstance().get(AppConfig.UNIT_TEST_MODE)=" +
        Main.getAppConfigInstance().get(AppConfig.UNIT_TEST_MODE));
    if(System.getProperty("test.warehouse.dir") != null) {
      /*when running in unit test mode, pass this property to HCat,
      which will in turn pass it to Hive to make sure that Hive
      tries to write to a directory that exists.*/
      args.add("-D");
      args.add("hive.metastore.warehouse.dir=" + System.getProperty("test.warehouse.dir"));
    }
    return args;
  }

  /**
   * Return a json description of the database.
   */
  public Response descDatabase(String user, String db, boolean extended)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "desc database " + db + "; ";
    if (extended)
      exec = "desc database extended " + db + "; ";

    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(res).build();
    } catch (HcatException e) {
      throw new HcatException("unable to describe database: " + db,
        e.execBean, exec);
    }
  }

  /**
   * Return a json "show databases like".  This will return a list of
   * databases.
   */
  public Response listDatabases(String user, String dbPattern)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("show databases like '%s';", dbPattern);
    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(res)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to show databases for: " + dbPattern,
        e.execBean, exec);
    }
  }

  /**
   * Create a database with the given name
   */
  public Response createDatabase(String user, DatabaseDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "create database";
    if (desc.ifNotExists)
      exec += " if not exists";
    exec += " " + desc.database;
    if (TempletonUtils.isset(desc.comment))
      exec += String.format(" comment '%s'", desc.comment);
    if (TempletonUtils.isset(desc.location))
      exec += String.format(" location '%s'", desc.location);
    if (TempletonUtils.isset(desc.properties))
      exec += String.format(" with dbproperties (%s)",
        makePropertiesStatement(desc.properties));
    exec += ";";

    String res = jsonRun(user, exec, desc.group, desc.permissions);
    return JsonBuilder.create(res)
      .put("database", desc.database)
      .build();
  }

  /**
   * Drop the given database
   */
  public Response dropDatabase(String user, String db,
                 boolean ifExists, String option,
                 String group, String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "drop database";
    if (ifExists)
      exec += " if exists";
    exec += " " + db;
    if (TempletonUtils.isset(option))
      exec += " " + option;
    exec += ";";

    String res = jsonRun(user, exec, group, permissions);
    return JsonBuilder.create(res)
      .put("database", db)
      .build();
  }

  /**
   * Create a table.
   */
  public Response createTable(String user, String db, TableDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = makeCreateTable(db, desc);

    try {
      String res = jsonRun(user, exec, desc.group, desc.permissions, true);

      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", desc.table)
        .build();
    } catch (final HcatException e) {
      throw new HcatException("unable to create table: " + desc.table,
        e.execBean, exec);
    }
  }

  /**
   * Create a table like another.
   */
  public Response createTableLike(String user, String db, TableLikeDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; create", db);

    if (desc.external)
      exec += " external";
    exec += String.format(" table %s like %s", desc.newTable, desc.existingTable);
    if (TempletonUtils.isset(desc.location))
      exec += String.format(" location '%s'", desc.location);
    exec += ";";

    try {
      String res = jsonRun(user, exec, desc.group, desc.permissions, true);

      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", desc.newTable)
        .build();
    } catch (final HcatException e) {
      throw new HcatException("unable to create table: " + desc.newTable,
        e.execBean, exec);
    }
  }

  /**
   * Return a json description of the table.
   */
  public Response descTable(String user, String db, String table, boolean extended)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "use " + db + "; ";
    if (extended)
      exec += "desc extended " + table + "; ";
    else
      exec += "desc " + table + "; ";
    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to describe database: " + db,
        e.execBean, exec);
    }
  }

  /**
   * Return a json "show table like".  This will return a list of
   * tables.
   */
  public Response listTables(String user, String db, String tablePattern)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; show tables like '%s';",
      db, tablePattern);
    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(res)
        .put("database", db)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to show tables for: " + tablePattern,
        e.execBean, exec);
    }
  }

  /**
   * Return a json "show table extended like" with extra info from "desc exteded"
   * This will return table with exact name match.
   */
  public Response descExtendedTable(String user, String db, String table)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; show table extended like %s;",
      db, table);
    try {
      //get detailed "tableInfo" from query "desc extended tablename;"
      Response res0 = descTable(user, db, table, true);
      if (res0.getStatus() != HttpStatus.OK_200)
          return res0;
      Map m = (Map) res0.getEntity();
      Map tableInfo = (Map) m.get("tableInfo");

      String res = jsonRun(user, exec);
      JsonBuilder jb = JsonBuilder.create(singleTable(res, table))
        .remove("tableName")
        .put("database", db)
        .put("table", table)
        .put("retention", tableInfo.get("retention"))
        .put("sd", tableInfo.get("sd"))
        .put("parameters", tableInfo.get("parameters"))
        .put("parametersSize", tableInfo.get("parametersSize"))
        .put("tableType", tableInfo.get("tableType"));

      // If we can get them from HDFS, add group and permission
      String loc = (String) jb.getMap().get("location");
      if (loc != null && loc.startsWith("hdfs://")) {
        try {
          FileSystem fs = FileSystem.get(appConf);
          FileStatus status = fs.getFileStatus(new Path(new URI(loc)));
          jb.put("group", status.getGroup());
          jb.put("permission", status.getPermission().toString());
        } catch (Exception e) {
          LOG.warn(e.getMessage() + " Couldn't get permissions for " + loc);
        }
      }
      return jb.build();
    } catch (HcatException e) {
      throw new HcatException("unable to show table: " + table, e.execBean, exec);
    }
  }

  // Format a list of Columns for a create statement
  private String makeCols(List<ColumnDesc> cols) {
    ArrayList<String> res = new ArrayList<String>();
    for (ColumnDesc col : cols)
      res.add(makeOneCol(col));
    return StringUtils.join(res, ", ");
  }

  // Format a Column for a create statement
  private String makeOneCol(ColumnDesc col) {
    String res = String.format("%s %s", col.name, col.type);
    if (TempletonUtils.isset(col.comment))
      res += String.format(" comment '%s'", col.comment);
    return res;
  }

  // Make a create table statement
  private String makeCreateTable(String db, TableDesc desc) {
    String exec = String.format("use %s; create", db);

    if (desc.external)
      exec += " external";
    exec += " table";
    if (desc.ifNotExists)
      exec += " if not exists";
    exec += " " + desc.table;

    if (TempletonUtils.isset(desc.columns))
      exec += String.format("(%s)", makeCols(desc.columns));
    if (TempletonUtils.isset(desc.comment))
      exec += String.format(" comment '%s'", desc.comment);
    if (TempletonUtils.isset(desc.partitionedBy))
      exec += String.format(" partitioned by (%s)", makeCols(desc.partitionedBy));
    if (desc.clusteredBy != null)
      exec += String.format(" clustered by %s", makeClusteredBy(desc.clusteredBy));
    if (desc.format != null)
      exec += " " + makeStorageFormat(desc.format);
    if (TempletonUtils.isset(desc.location))
      exec += String.format(" location '%s'", desc.location);
    if (TempletonUtils.isset(desc.tableProperties))
      exec += String.format(" tblproperties (%s)",
        makePropertiesStatement(desc.tableProperties));
    exec += ";";

    return exec;
  }

  // Format a clustered by statement
  private String makeClusteredBy(TableDesc.ClusteredByDesc desc) {
    String res = String.format("(%s)", StringUtils.join(desc.columnNames, ", "));
    if (TempletonUtils.isset(desc.sortedBy))
      res += String.format(" sorted by (%s)", makeClusterSortList(desc.sortedBy));
    res += String.format(" into %s buckets", desc.numberOfBuckets);

    return res;
  }

  // Format a sorted by statement
  private String makeClusterSortList(List<TableDesc.ClusterSortOrderDesc> descs) {
    ArrayList<String> res = new ArrayList<String>();
    for (TableDesc.ClusterSortOrderDesc desc : descs)
      res.add(makeOneClusterSort(desc));
    return StringUtils.join(res, ", ");
  }

  // Format a single cluster sort statement
  private String makeOneClusterSort(TableDesc.ClusterSortOrderDesc desc) {
    return String.format("%s %s", desc.columnName, desc.order.toString());
  }

  // Format the storage format statements
  private String makeStorageFormat(TableDesc.StorageFormatDesc desc) {
    String res = "";

    if (desc.rowFormat != null)
      res += makeRowFormat(desc.rowFormat);
    if (TempletonUtils.isset(desc.storedAs))
      res += String.format(" stored as %s", desc.storedAs);
    if (desc.storedBy != null)
      res += " " + makeStoredBy(desc.storedBy);

    return res;
  }

  // Format the row format statement
  private String makeRowFormat(TableDesc.RowFormatDesc desc) {
    String res =
      makeTermBy(desc.fieldsTerminatedBy, "fields")
        + makeTermBy(desc.collectionItemsTerminatedBy, "collection items")
        + makeTermBy(desc.mapKeysTerminatedBy, "map keys")
        + makeTermBy(desc.linesTerminatedBy, "lines");

    if (TempletonUtils.isset(res))
      return "row format delimited" + res;
    else if (desc.serde != null)
      return makeSerdeFormat(desc.serde);
    else
      return "";
  }

  // A row format terminated by clause
  private String makeTermBy(String sep, String fieldName) {

    if (TempletonUtils.isset(sep))
      return String.format(" %s terminated by '%s'", fieldName, sep);
    else
      return "";
  }

  // Format the serde statement
  private String makeSerdeFormat(TableDesc.SerdeDesc desc) {
    String res = "row format serde " + desc.name;
    if (TempletonUtils.isset(desc.properties))
      res += String.format(" with serdeproperties (%s)",
        makePropertiesStatement(desc.properties));
    return res;
  }

  // Format the properties statement
  private String makePropertiesStatement(Map<String, String> properties) {
    ArrayList<String> res = new ArrayList<String>();
    for (Map.Entry<String, String> e : properties.entrySet())
      res.add(String.format("'%s'='%s'", e.getKey(), e.getValue()));
    return StringUtils.join(res, ", ");
  }

  // Format the stored by statement
  private String makeStoredBy(TableDesc.StoredByDesc desc) {
    String res = String.format("stored by '%s'", desc.className);
    if (TempletonUtils.isset(desc.properties))
      res += String.format(" with serdeproperties (%s)",
        makePropertiesStatement(desc.properties));
    return res;
  }

  // Pull out the first table from the "show extended" json.
  private String singleTable(String json, String table)
    throws IOException {
    Map obj = JsonBuilder.jsonToMap(json);
    if (JsonBuilder.isError(obj))
      return json;

    List tables = (List) obj.get("tables");
    if (TempletonUtils.isset(tables))
      return JsonBuilder.mapToJson(tables.get(0));
    else {
      return JsonBuilder
        .createError(ErrorMsg.INVALID_TABLE.format(table),
            ErrorMsg.INVALID_TABLE.getErrorCode()).
          buildJson();
    }
  }

  /**
   * Drop a table.
   */
  public Response dropTable(String user, String db,
                String table, boolean ifExists,
                String group, String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; drop table", db);
    if (ifExists)
      exec += " if exists";
    exec += String.format(" %s;", table);

    try {
      String res = jsonRun(user, exec, group, permissions, true);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to drop table: " + table, e.execBean, exec);
    }
  }

  /**
   * Rename a table.
   */
  public Response renameTable(String user, String db,
                String oldTable, String newTable,
                String group, String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; alter table %s rename to %s;",
      db, oldTable, newTable);
    try {
      String res = jsonRun(user, exec, group, permissions, true);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", newTable)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to rename table: " + oldTable,
        e.execBean, exec);
    }
  }

  /**
   * Describe one table property.
   */
  public Response descTableProperty(String user, String db,
                    String table, String property)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    Response res = descTable(user, db, table, true);
    if (res.getStatus() != HttpStatus.OK_200)
      return res;
    Map props = tableProperties(res.getEntity());
    Map found = null;
    if (props != null) {
      String value = (String) props.get(property);
      if (value != null) {
        found = new HashMap<String, String>();
        found.put(property, value);
      }
    }

    return JsonBuilder.create()
      .put("database", db)
      .put("table", table)
      .put("property", found)
      .build();
  }

  /**
   * List the table properties.
   */
  public Response listTableProperties(String user, String db, String table)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    Response res = descTable(user, db, table, true);
    if (res.getStatus() != HttpStatus.OK_200)
      return res;
    Map props = tableProperties(res.getEntity());
    return JsonBuilder.create()
      .put("database", db)
      .put("table", table)
      .put("properties", props)
      .build();
  }

  /**
   * Add one table property.
   */
  public Response addOneTableProperty(String user, String db, String table,
                    TablePropertyDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec
      = String.format("use %s; alter table %s set tblproperties ('%s'='%s');",
        db, table, desc.name, desc.value);
    try {
      String res = jsonRun(user, exec, desc.group, desc.permissions, true);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .put("property", desc.name)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to add table property: " + table,
        e.execBean, exec);
    }
  }

  private Map tableProperties(Object extendedTable) {
    if (!(extendedTable instanceof Map))
      return null;
    Map m = (Map) extendedTable;
    Map tableInfo = (Map) m.get("tableInfo");
    if (tableInfo == null)
      return null;

    return (Map) tableInfo.get("parameters");
  }

  /**
   * Return a json description of the partitions.
   */
  public Response listPartitions(String user, String db, String table)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "use " + db + "; ";
    exec += "show partitions " + table + "; ";
    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to show partitions for table: " + table,
        e.execBean, exec);
    }
  }

  /**
   * Return a json description of one partition.
   */
  public Response descOnePartition(String user, String db, String table,
                   String partition)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = "use " + db + "; ";
    exec += "show table extended like " + table
      + " partition (" + partition + "); ";
    try {
      String res = jsonRun(user, exec);
      return JsonBuilder.create(singleTable(res, table))
        .remove("tableName")
        .put("database", db)
        .put("table", table)
        .put("partition", partition)
        .build();
    } catch (HcatException e) {
      if (e.execBean.stderr.contains("SemanticException") &&
        e.execBean.stderr.contains("Partition not found")) {
        String emsg = "Partition " + partition + " for table "
          + table + " does not exist" + db + "." + table + " does not exist";
        return JsonBuilder.create()
          .put("error", emsg)
           //this error should really be produced by Hive (DDLTask)
          .put("errorCode", ErrorMsg.INVALID_PARTITION.getErrorCode())
          .put("database", db)
          .put("table", table)
          .put("partition", partition)
          .build();
      }

      throw new HcatException("unable to show partition: "
        + table + " " + partition,
        e.execBean,
        exec);
    }
  }

  /**
   * Add one partition.
   */
  public Response addOnePartition(String user, String db, String table,
                  PartitionDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; alter table %s add", db, table);
    if (desc.ifNotExists)
      exec += " if not exists";
    exec += String.format(" partition (%s)", desc.partition);
    if (TempletonUtils.isset(desc.location))
      exec += String.format(" location '%s'", desc.location);
    exec += ";";
    try {
      String res = jsonRun(user, exec, desc.group, desc.permissions, true);
      if (res.indexOf("AlreadyExistsException") > -1) {
        return JsonBuilder.create().
          put("error", "Partition already exists")
           //This error code should really be produced by Hive
          .put("errorCode", ErrorMsg.PARTITION_EXISTS.getErrorCode())
          .put("database", db)
          .put("table", table)
          .put("partition", desc.partition).build();
      }
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .put("partition", desc.partition)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to add partition: " + desc,
        e.execBean, exec);
    }
  }

  /**
   * Drop a partition.
   */
  public Response dropPartition(String user, String db,
                  String table, String partition, boolean ifExists,
                  String group, String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; alter table %s drop", db, table);
    if (ifExists)
      exec += " if exists";
    exec += String.format(" partition (%s);", partition);

    try {
      String res = jsonRun(user, exec, group, permissions, true);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .put("partition", partition)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to drop partition: " + partition,
        e.execBean, exec);
    }
  }

  /**
   * Return a json description of the columns.  Same as
   * describeTable.
   */
  public Response listColumns(String user, String db, String table)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    try {
      return descTable(user, db, table, false);
    } catch (HcatException e) {
      throw new HcatException("unable to show columns for table: " + table,
        e.execBean, e.statement);
    }
  }

  /**
   * Return a json description of one column.
   */
  public Response descOneColumn(String user, String db, String table, String column)
    throws SimpleWebException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    Response res = listColumns(user, db, table);
    if (res.getStatus() != HttpStatus.OK_200)
      return res;

    Object o = res.getEntity();
    final Map fields = (o != null && (o instanceof Map)) ? (Map) o : null;
    if (fields == null)
      throw new SimpleWebException(HttpStatus.NOT_FOUND_404, "Internal error, unable to find column "
        + column);


    List<Map> cols = (List) fields.get("columns");
    Map found = null;
    if (cols != null) {
      for (Map col : cols) {
        if (column.equals(col.get("name"))) {
          found = col;
          break;
        }
      }
    }
    if (found == null)
      throw new SimpleWebException(HttpStatus.NOT_FOUND_404, "unable to find column " + column,
        new HashMap<String, Object>() {
          {
            put("description", fields);
          }
        });
    fields.remove("columns");
    fields.put("column", found);
    return Response.fromResponse(res).entity(fields).build();
  }

  /**
   * Add one column.
   */
  public Response addOneColumn(String user, String db, String table,
                 ColumnDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    String exec = String.format("use %s; alter table %s add columns (%s %s",
      db, table, desc.name, desc.type);
    if (TempletonUtils.isset(desc.comment))
      exec += String.format(" comment '%s'", desc.comment);
    exec += ");";
    try {
      String res = jsonRun(user, exec, desc.group, desc.permissions, true);
      return JsonBuilder.create(res)
        .put("database", db)
        .put("table", table)
        .put("column", desc.name)
        .build();
    } catch (HcatException e) {
      throw new HcatException("unable to add column: " + desc,
        e.execBean, exec);
    }
  }

  // Check that the hcat result is valid and or has a valid json
  // error
  private boolean isValid(ExecBean eb, boolean requireEmptyOutput) {
    if (eb == null)
      return false;

    try {
      Map m = JsonBuilder.jsonToMap(eb.stdout);
      if (m.containsKey("error")) // This is a valid error message.
        return true;
    } catch (IOException e) {
      return false;
    }

    if (eb.exitcode != 0)
      return false;

    if (requireEmptyOutput)
      if (TempletonUtils.isset(eb.stdout))
        return false;

    return true;
  }

  // Run an hcat expression and return just the json outout.
  private String jsonRun(String user, String exec,
               String group, String permissions,
               boolean requireEmptyOutput)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    ExecBean res = run(user, exec, true, group, permissions);

    if (!isValid(res, requireEmptyOutput))
      throw new HcatException("Failure calling hcat: " + exec, res, exec);

    return res.stdout;
  }

  // Run an hcat expression and return just the json outout.  No
  // permissions set.
  private String jsonRun(String user, String exec)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    return jsonRun(user, exec, null, null);
  }

  // Run an hcat expression and return just the json outout.
  private String jsonRun(String user, String exec,
               String group, String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    ExecuteException, IOException {
    return jsonRun(user, exec, group, permissions, false);
  }
}
