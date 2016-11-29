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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.exec.ExecuteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hive.hcatalog.templeton.LauncherDelegator.JobType;
import org.apache.hive.hcatalog.templeton.tool.TempletonUtils;

/**
 * The Templeton Web API server.
 */
@Path("/v1")
public class Server {
  public static final String VERSION = "v1";
  public static final String DO_AS_PARAM = "doAs";

  /**
   * The status message.  Always "ok"
   */
  public static final Map<String, String> STATUS_OK = createStatusMsg();

  /**
   * The list of supported api versions.
   */
  public static final Map<String, Object> SUPPORTED_VERSIONS = createVersions();

  /**
   * The list of supported return formats.  Always json.
   */
  public static final Map<String, Object> SUPPORTED_FORMATS = createFormats();

  // Build the status message for the /status call.
  private static Map<String, String> createStatusMsg() {
    HashMap<String, String> res = new HashMap<String, String>();
    res.put("status", "ok");
    res.put("version", VERSION);

    return Collections.unmodifiableMap(res);
  }

  // Build the versions list.
  private static Map<String, Object> createVersions() {
    ArrayList<String> versions = new ArrayList<String>();
    versions.add(VERSION);

    HashMap<String, Object> res = new HashMap<String, Object>();
    res.put("supportedVersions", versions);
    res.put("version", VERSION);

    return Collections.unmodifiableMap(res);
  }

  // Build the supported formats list
  private static Map<String, Object> createFormats() {
    ArrayList<String> formats = new ArrayList<String>();
    formats.add(MediaType.APPLICATION_JSON);
    HashMap<String, Object> res = new HashMap<String, Object>();
    res.put("responseTypes", formats);

    return Collections.unmodifiableMap(res);
  }

  protected static ExecService execService = ExecServiceImpl.getInstance();
  private static AppConfig appConf = Main.getAppConfigInstance();

  // The SecurityContext set by AuthFilter
  private
  @Context
  SecurityContext theSecurityContext;

  // The uri requested
  private
  @Context
  UriInfo theUriInfo;
  private @QueryParam(DO_AS_PARAM) String doAs;
  private @Context HttpServletRequest request;

  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  /**
   * Check the status of this server.  Always OK.
   */
  @GET
  @Path("status")
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String, String> status() {
    return STATUS_OK;
  }

  /**
   * Check the supported request formats of this server.
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String, Object> requestFormats() {
    return SUPPORTED_FORMATS;
  }

  /**
   * Check the version(s) supported by this server.
   */
  @GET
  @Path("version")
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String, Object> version() {
    return SUPPORTED_VERSIONS;
  }

  /**
   * Get version of hadoop software being run by this WebHCat server
   */
  @GET
  @Path("version/hadoop")
  @Produces(MediaType.APPLICATION_JSON)
  public Response hadoopVersion()  throws IOException {
    VersionDelegator d = new VersionDelegator(appConf);
    return d.getVersion("hadoop");
  }

  /**
   * Get version of hive software being run by this WebHCat server
   */
  @GET
  @Path("version/hive")
  @Produces(MediaType.APPLICATION_JSON)
  public Response hiveVersion()  throws IOException {
    VersionDelegator d = new VersionDelegator(appConf);
    return d.getVersion("hive");
  }

  /**
   * Get version of sqoop software being run by this WebHCat server
   */
  @GET
  @Path("version/sqoop")
  @Produces(MediaType.APPLICATION_JSON)
  public Response sqoopVersion()  throws IOException {
    VersionDelegator d = new VersionDelegator(appConf);
    return d.getVersion("sqoop");
  }

  /**
   * Get version of pig software being run by this WebHCat server
   */
  @GET
  @Path("version/pig")
  @Produces(MediaType.APPLICATION_JSON)
  public Response pigVersion()  throws IOException {
    VersionDelegator d = new VersionDelegator(appConf);
    return d.getVersion("pig");
  }

  /**
   * Execute an hcat ddl expression on the local box.  It is run
   * as the authenticated user and rate limited.
   */
  @POST
  @Path("ddl")
  @Produces({MediaType.APPLICATION_JSON})
  public ExecBean ddl(@FormParam("exec") String exec,
            @FormParam("group") String group,
            @FormParam("permissions") String permissions)
    throws NotAuthorizedException, BusyException, BadParam,
    ExecuteException, IOException {
    verifyUser();
    verifyParam(exec, "exec");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.run(getDoAsUser(), exec, false, group, permissions);
  }

  /**
   * List all the tables in an hcat database.
   */
  @GET
  @Path("ddl/database/{db}/table")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTables(@PathParam("db") String db,
                 @QueryParam("like") String tablePattern)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    if (!TempletonUtils.isset(tablePattern)) {
      tablePattern = "*";
    }
    return d.listTables(getDoAsUser(), db, tablePattern);
  }

  /**
   * Create a new table.
   */
  @PUT
  @Path("ddl/database/{db}/table/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createTable(@PathParam("db") String db,
                @PathParam("table") String table,
                TableDesc desc)
    throws SimpleWebException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    desc.table = table;

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.createTable(getDoAsUser(), db, desc);
  }

  /**
   * Create a new table like another table.
   */
  @PUT
  @Path("ddl/database/{db}/table/{existingTable}/like/{newTable}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createTableLike(@PathParam("db") String db,
                  @PathParam("existingTable") String existingTable,
                  @PathParam("newTable") String newTable,
                  TableLikeDesc desc)
    throws SimpleWebException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(existingTable, ":existingTable");
    verifyDdlParam(newTable, ":newTable");
    desc.existingTable = existingTable;
    desc.newTable = newTable;

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.createTableLike(getDoAsUser(), db, desc);
  }

  /**
   * Describe an hcat table.  This is normally a simple list of
   * columns (using "desc table"), but the extended format will show
   * more information (using "show table extended like").
   */
  @GET
  @Path("ddl/database/{db}/table/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response descTable(@PathParam("db") String db,
                @PathParam("table") String table,
                @QueryParam("format") String format)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    if ("extended".equals(format)) {
      return d.descExtendedTable(getDoAsUser(), db, table);
    }
    else {
      return d.descTable(getDoAsUser(), db, table, false);
    }
  }

  /**
   * Drop an hcat table.
   */
  @DELETE
  @Path("ddl/database/{db}/table/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropTable(@PathParam("db") String db,
                @PathParam("table") String table,
                @QueryParam("ifExists") boolean ifExists,
                @QueryParam("group") String group,
                @QueryParam("permissions") String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.dropTable(getDoAsUser(), db, table, ifExists, group, permissions);
  }

  /**
   * Rename an hcat table.
   */
  @POST
  @Path("ddl/database/{db}/table/{table}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response renameTable(@PathParam("db") String db,
                @PathParam("table") String oldTable,
                @FormParam("rename") String newTable,
                @FormParam("group") String group,
                @FormParam("permissions") String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(oldTable, ":table");
    verifyDdlParam(newTable, "rename");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.renameTable(getDoAsUser(), db, oldTable, newTable, group, permissions);
  }

  /**
   * Describe a single property on an hcat table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/property/{property}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response descOneTableProperty(@PathParam("db") String db,
                     @PathParam("table") String table,
                     @PathParam("property") String property)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyDdlParam(property, ":property");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.descTableProperty(getDoAsUser(), db, table, property);
  }

  /**
   * List all the properties on an hcat table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/property")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listTableProperties(@PathParam("db") String db,
                    @PathParam("table") String table)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.listTableProperties(getDoAsUser(), db, table);
  }

  /**
   * Add a single property on an hcat table.
   */
  @PUT
  @Path("ddl/database/{db}/table/{table}/property/{property}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response addOneTableProperty(@PathParam("db") String db,
                    @PathParam("table") String table,
                    @PathParam("property") String property,
                    TablePropertyDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyDdlParam(property, ":property");
    desc.name = property;

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.addOneTableProperty(getDoAsUser(), db, table, desc);
  }

  /**
   * List all the partitions in an hcat table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/partition")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listPartitions(@PathParam("db") String db,
                   @PathParam("table") String table)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.listPartitions(getDoAsUser(), db, table);
  }

  /**
   * Describe a single partition in an hcat table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/partition/{partition}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response descPartition(@PathParam("db") String db,
                  @PathParam("table") String table,
                  @PathParam("partition") String partition)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyParam(partition, ":partition");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.descOnePartition(getDoAsUser(), db, table, partition);
  }

  /**
   * Create a partition in an hcat table.
   */
  @PUT
  @Path("ddl/database/{db}/table/{table}/partition/{partition}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response addOnePartition(@PathParam("db") String db,
                  @PathParam("table") String table,
                  @PathParam("partition") String partition,
                  PartitionDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyParam(partition, ":partition");
    desc.partition = partition;
    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.addOnePartition(getDoAsUser(), db, table, desc);
  }

  /**
   * Drop a partition in an hcat table.
   */
  @DELETE
  @Path("ddl/database/{db}/table/{table}/partition/{partition}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropPartition(@PathParam("db") String db,
                  @PathParam("table") String table,
                  @PathParam("partition") String partition,
                  @QueryParam("ifExists") boolean ifExists,
                  @QueryParam("group") String group,
                  @QueryParam("permissions") String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyParam(partition, ":partition");
    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.dropPartition(getDoAsUser(), db, table, partition, ifExists,
        group, permissions);
  }

  /**
   * List all databases, or those that match a pattern.
   */
  @GET
  @Path("ddl/database/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listDatabases(@QueryParam("like") String dbPattern)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();

    HcatDelegator d = new HcatDelegator(appConf, execService);
    if (!TempletonUtils.isset(dbPattern)) {
      dbPattern = "*";
    }
    return d.listDatabases(getDoAsUser(), dbPattern);
  }

  /**
   * Describe a database
   */
  @GET
  @Path("ddl/database/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response descDatabase(@PathParam("db") String db,
                 @QueryParam("format") String format)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.descDatabase(getDoAsUser(), db, "extended".equals(format));
  }

  /**
   * Create a database
   */
  @PUT
  @Path("ddl/database/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response createDatabase(@PathParam("db") String db,
                   DatabaseDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    desc.database = db;
    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.createDatabase(getDoAsUser(), desc);
  }

  /**
   * Drop a database
   */
  @DELETE
  @Path("ddl/database/{db}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropDatabase(@PathParam("db") String db,
                 @QueryParam("ifExists") boolean ifExists,
                 @QueryParam("option") String option,
                 @QueryParam("group") String group,
                 @QueryParam("permissions") String permissions)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    if (TempletonUtils.isset(option)) {
      verifyDdlParam(option, "option");
    }
    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.dropDatabase(getDoAsUser(), db, ifExists, option,
        group, permissions);
  }

  /**
   * List the columns in an hcat table.  Currently the same as
   * describe table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/column")
  @Produces(MediaType.APPLICATION_JSON)
  public Response listColumns(@PathParam("db") String db,
                @PathParam("table") String table)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.listColumns(getDoAsUser(), db, table);
  }

  /**
   * Describe a single column in an hcat table.
   */
  @GET
  @Path("ddl/database/{db}/table/{table}/column/{column}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response descColumn(@PathParam("db") String db,
                 @PathParam("table") String table,
                 @PathParam("column") String column)
    throws SimpleWebException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyParam(column, ":column");

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.descOneColumn(getDoAsUser(), db, table, column);
  }

  /**
   * Create a column in an hcat table.
   */
  @PUT
  @Path("ddl/database/{db}/table/{table}/column/{column}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response addOneColumn(@PathParam("db") String db,
                 @PathParam("table") String table,
                 @PathParam("column") String column,
                 ColumnDesc desc)
    throws HcatException, NotAuthorizedException, BusyException,
    BadParam, ExecuteException, IOException {
    verifyUser();
    verifyDdlParam(db, ":db");
    verifyDdlParam(table, ":table");
    verifyParam(column, ":column");
    verifyParam(desc.type, "type");
    desc.name = column;

    HcatDelegator d = new HcatDelegator(appConf, execService);
    return d.addOneColumn(getDoAsUser(), db, table, desc);
  }

  /**
   * Run a MapReduce Streaming job.
   * @param callback URL which WebHCat will call when the hive job finishes
   */
  @POST
  @Path("mapreduce/streaming")
  @Produces({MediaType.APPLICATION_JSON})
  public EnqueueBean mapReduceStreaming(@FormParam("input") List<String> inputs,
		      @FormParam("inputreader") String inputreader,
                      @FormParam("output") String output,
                      @FormParam("mapper") String mapper,
                      @FormParam("reducer") String reducer,
                      @FormParam("combiner") String combiner,
                      @FormParam("file") List<String> fileList,
                      @FormParam("files") String files,
                      @FormParam("define") List<String> defines,
                      @FormParam("cmdenv") List<String> cmdenvs,
                      @FormParam("arg") List<String> args,
                      @FormParam("statusdir") String statusdir,
                      @FormParam("callback") String callback,
                      @FormParam("enablelog") boolean enablelog,
                      @FormParam("enablejobreconnect") Boolean enablejobreconnect)
    throws NotAuthorizedException, BusyException, BadParam, QueueException,
    ExecuteException, IOException, InterruptedException {
    verifyUser();
    verifyParam(inputs, "input");
    verifyParam(mapper, "mapper");
    verifyParam(reducer, "reducer");

    Map<String, Object> userArgs = new HashMap<String, Object>();
    userArgs.put("user.name", getDoAsUser());
    userArgs.put("input", inputs);
    userArgs.put("inputreader", inputreader);
    userArgs.put("output", output);
    userArgs.put("mapper", mapper);
    userArgs.put("reducer", reducer);
    userArgs.put("combiner", combiner);
    userArgs.put("file",  fileList);
    userArgs.put("files",  files);
    userArgs.put("define",  defines);
    userArgs.put("cmdenv",  cmdenvs);
    userArgs.put("arg",  args);
    userArgs.put("statusdir", statusdir);
    userArgs.put("callback", callback);
    userArgs.put("enablelog", Boolean.toString(enablelog));
    userArgs.put("enablejobreconnect", enablejobreconnect);
    checkEnableLogPrerequisite(enablelog, statusdir);

    StreamingDelegator d = new StreamingDelegator(appConf);
    return d.run(getDoAsUser(), userArgs, inputs, inputreader, output, mapper, reducer, combiner,
      fileList, files, defines, cmdenvs, args,
      statusdir, callback, getCompletedUrl(), enablelog, enablejobreconnect, JobType.STREAMING);
  }

  /**
   * Run a MapReduce Jar job.
   * Params correspond to the REST api params
   * @param  usesHcatalog if {@code true}, means the Jar uses HCat and thus needs to access
   *    metastore, which requires additional steps for WebHCat to perform in a secure cluster.
   * @param callback URL which WebHCat will call when the hive job finishes
   * @see org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob
   */
  @POST
  @Path("mapreduce/jar")
  @Produces({MediaType.APPLICATION_JSON})
  public EnqueueBean mapReduceJar(@FormParam("jar") String jar,
                  @FormParam("class") String mainClass,
                  @FormParam("libjars") String libjars,
                  @FormParam("files") String files,
                  @FormParam("arg") List<String> args,
                  @FormParam("define") List<String> defines,
                  @FormParam("statusdir") String statusdir,
                  @FormParam("callback") String callback,
                  @FormParam("usehcatalog") boolean usesHcatalog,
                  @FormParam("enablelog") boolean enablelog,
                  @FormParam("enablejobreconnect") Boolean enablejobreconnect)
    throws NotAuthorizedException, BusyException, BadParam, QueueException,
    ExecuteException, IOException, InterruptedException {
    verifyUser();
    verifyParam(jar, "jar");
    verifyParam(mainClass, "class");

    Map<String, Object> userArgs = new HashMap<String, Object>();
    userArgs.put("user.name", getDoAsUser());
    userArgs.put("jar", jar);
    userArgs.put("class", mainClass);
    userArgs.put("libjars", libjars);
    userArgs.put("files", files);
    userArgs.put("arg", args);
    userArgs.put("define", defines);
    userArgs.put("statusdir", statusdir);
    userArgs.put("callback", callback);
    userArgs.put("enablelog", Boolean.toString(enablelog));
    userArgs.put("enablejobreconnect", enablejobreconnect);

    checkEnableLogPrerequisite(enablelog, statusdir);

    JarDelegator d = new JarDelegator(appConf);
    return d.run(getDoAsUser(), userArgs,
      jar, mainClass,
      libjars, files, args, defines,
      statusdir, callback, usesHcatalog, getCompletedUrl(), enablelog, enablejobreconnect, JobType.JAR);
  }

  /**
   * Run a Pig job.
   * Params correspond to the REST api params.  If '-useHCatalog' is in the {@code pigArgs, usesHcatalog},
   * is interpreted as true.
   * @param  usesHcatalog if {@code true}, means the Pig script uses HCat and thus needs to access
   *    metastore, which requires additional steps for WebHCat to perform in a secure cluster.
   *    This does nothing to ensure that Pig is installed on target node in the cluster.
   * @param callback URL which WebHCat will call when the hive job finishes
   * @see org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob
   */
  @POST
  @Path("pig")
  @Produces({MediaType.APPLICATION_JSON})
  public EnqueueBean pig(@FormParam("execute") String execute,
               @FormParam("file") String srcFile,
               @FormParam("arg") List<String> pigArgs,
               @FormParam("files") String otherFiles,
               @FormParam("statusdir") String statusdir,
               @FormParam("callback") String callback,
               @FormParam("usehcatalog") boolean usesHcatalog,
               @FormParam("enablelog") boolean enablelog,
               @FormParam("enablejobreconnect") Boolean enablejobreconnect)
    throws NotAuthorizedException, BusyException, BadParam, QueueException,
    ExecuteException, IOException, InterruptedException {
    verifyUser();
    if (execute == null && srcFile == null) {
      throw new BadParam("Either execute or file parameter required");
    }

    //add all function arguments to a map
    Map<String, Object> userArgs = new HashMap<String, Object>();
    userArgs.put("user.name", getDoAsUser());
    userArgs.put("execute", execute);
    userArgs.put("file", srcFile);
    userArgs.put("arg", pigArgs);
    userArgs.put("files", otherFiles);
    userArgs.put("statusdir", statusdir);
    userArgs.put("callback", callback);
    userArgs.put("enablelog", Boolean.toString(enablelog));
    userArgs.put("enablejobreconnect", enablejobreconnect);

    checkEnableLogPrerequisite(enablelog, statusdir);

    PigDelegator d = new PigDelegator(appConf);
    return d.run(getDoAsUser(), userArgs,
      execute, srcFile,
      pigArgs, otherFiles,
      statusdir, callback, usesHcatalog, getCompletedUrl(), enablelog, enablejobreconnect);
  }

   /**
   * Run a Sqoop job.
   * @param optionsFile  name of option file which contains Sqoop command to run
   * @param otherFiles   additional files to be shipped to the launcher, such as option
                         files which contain part of the Sqoop command
   * @param libdir       dir containing JDBC jars that Sqoop will need to interact with the database
   * @param statusdir    where the stderr/stdout of templeton controller job goes
   * @param callback     URL which WebHCat will call when the sqoop job finishes
   * @param enablelog    whether to collect mapreduce log into statusdir/logs
   * @param enablejobreconnect    whether to reconnect to a running child job on templeton
   *                              controller job retry
   */
  @POST
  @Path("sqoop")
  @Produces({MediaType.APPLICATION_JSON})
  public EnqueueBean sqoop(@FormParam("command") String command,
              @FormParam("optionsfile") String optionsFile,
              @FormParam("libdir") String libdir,
              @FormParam("files") String otherFiles,
              @FormParam("statusdir") String statusdir,
              @FormParam("callback") String callback,
              @FormParam("enablelog") boolean enablelog,
              @FormParam("enablejobreconnect") Boolean enablejobreconnect)
    throws NotAuthorizedException, BusyException, BadParam, QueueException,
    IOException, InterruptedException {
    verifyUser();
    if (command == null && optionsFile == null)
      throw new BadParam("Must define Sqoop command or a optionsfile contains Sqoop command to run Sqoop job.");
    if (command != null && optionsFile != null)
      throw new BadParam("Cannot set command and optionsfile at the same time.");
    checkEnableLogPrerequisite(enablelog, statusdir);

    //add all function arguments to a map
    Map<String, Object> userArgs = new HashMap<String, Object>();
    userArgs.put("user.name", getDoAsUser());
    userArgs.put("command", command);
    userArgs.put("optionsfile", optionsFile);
    userArgs.put("libdir", libdir);
    userArgs.put("files", otherFiles);
    userArgs.put("statusdir", statusdir);
    userArgs.put("callback", callback);
    userArgs.put("enablelog", Boolean.toString(enablelog));
    userArgs.put("enablejobreconnect", enablejobreconnect);
    SqoopDelegator d = new SqoopDelegator(appConf);
    return d.run(getDoAsUser(), userArgs, command, optionsFile, otherFiles,
      statusdir, callback, getCompletedUrl(), enablelog, enablejobreconnect, libdir);
  }

  /**
   * Run a Hive job.
   * @param execute    SQL statement to run, equivalent to "-e" from hive command line
   * @param srcFile    name of hive script file to run, equivalent to "-f" from hive
   *                   command line
   * @param hiveArgs   additional command line argument passed to the hive command line.
   *                   Please check https://cwiki.apache.org/Hive/languagemanual-cli.html
   *                   for detailed explanation of command line arguments
   * @param otherFiles additional files to be shipped to the launcher, such as the jars
   *                   used in "add jar" statement in hive script
   * @param defines    shortcut for command line arguments "--define"
   * @param statusdir  where the stderr/stdout of templeton controller job goes
   * @param callback   URL which WebHCat will call when the hive job finishes
   * @param enablelog  whether to collect mapreduce log into statusdir/logs
   * @param enablejobreconnect    whether to reconnect to a running child job on templeton
   *                              controller job retry
   */
  @POST
  @Path("hive")
  @Produces({MediaType.APPLICATION_JSON})
  public EnqueueBean hive(@FormParam("execute") String execute,
              @FormParam("file") String srcFile,
              @FormParam("arg") List<String> hiveArgs,
              @FormParam("files") String otherFiles,
              @FormParam("define") List<String> defines,
              @FormParam("statusdir") String statusdir,
              @FormParam("callback") String callback,
              @FormParam("enablelog") boolean enablelog,
              @FormParam("enablejobreconnect") Boolean enablejobreconnect)
    throws NotAuthorizedException, BusyException, BadParam, QueueException,
    ExecuteException, IOException, InterruptedException {
    verifyUser();
    if (execute == null && srcFile == null) {
      throw new BadParam("Either execute or file parameter required");
    }

    //add all function arguments to a map
    Map<String, Object> userArgs = new HashMap<String, Object>();
    userArgs.put("user.name", getDoAsUser());
    userArgs.put("execute", execute);
    userArgs.put("file", srcFile);
    userArgs.put("define", defines);
    userArgs.put("files", otherFiles);
    userArgs.put("statusdir", statusdir);
    userArgs.put("callback", callback);
    userArgs.put("enablelog", Boolean.toString(enablelog));
    userArgs.put("enablejobreconnect", enablejobreconnect);

    checkEnableLogPrerequisite(enablelog, statusdir);

    HiveDelegator d = new HiveDelegator(appConf);
    return d.run(getDoAsUser(), userArgs, execute, srcFile, defines, hiveArgs, otherFiles,
      statusdir, callback, getCompletedUrl(), enablelog, enablejobreconnect);
  }

  /**
   * Return the status of the jobid.
   */
  @GET
  @Path("jobs/{jobid}")
  @Produces({MediaType.APPLICATION_JSON})
  public QueueStatusBean showJobId(@PathParam("jobid") String jobid)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException {

    verifyUser();
    verifyParam(jobid, ":jobid");

    StatusDelegator d = new StatusDelegator(appConf);
    return d.run(getDoAsUser(), jobid);
  }

  /**
   * Kill a job in the queue.
   */
  @DELETE
  @Path("jobs/{jobid}")
  @Produces({MediaType.APPLICATION_JSON})
  public QueueStatusBean deleteJobId(@PathParam("jobid") String jobid)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException {

    verifyUser();
    verifyParam(jobid, ":jobid");

    DeleteDelegator d = new DeleteDelegator(appConf);
    return d.run(getDoAsUser(), jobid);
  }

  /**
   * Return all the known job ids for this user based on the optional filter conditions.
   * <p>
   * Example usages:
   * 1. curl -s 'http://localhost:50111/templeton/v1/jobs?user.name=hsubramaniyan'
   * Return all the Job IDs submitted by hsubramaniyan
   * 2. curl -s
   * 'http://localhost:50111/templeton/v1/jobs?user.name=hsubramaniyan%26showall=true'
   * Return all the Job IDs that are visible to hsubramaniyan
   * 3. curl -s
   * 'http://localhost:50111/templeton/v1/jobs?user.name=hsubramaniyan%26jobid=job_201312091733_0003'
   * Return all the Job IDs for hsubramaniyan after job_201312091733_0003.
   * 4. curl -s 'http://localhost:50111/templeton/v1/jobs?
   * user.name=hsubramaniyan%26jobid=job_201312091733_0003%26numrecords=5'
   * Return the first 5(atmost) Job IDs submitted by hsubramaniyan after job_201312091733_0003.
   * 5.  curl -s
   * 'http://localhost:50111/templeton/v1/jobs?user.name=hsubramaniyan%26numrecords=5'
   * Return the first 5(atmost) Job IDs submitted by hsubramaniyan after sorting the Job ID list
   * lexicographically.
   * </p>
   * <p>
   * Supporting pagination using "jobid" and "numrecords" parameters:
   * Step 1: Get the start "jobid" = job_xxx_000, "numrecords" = n
   * Step 2: Issue a curl command by specifying the user-defined "numrecords" and "jobid"
   * Step 3: If list obtained from Step 2 has size equal to "numrecords", retrieve the list's
   * last record and get the Job Id of the last record as job_yyy_k, else quit.
   * Step 4: set "jobid"=job_yyy_k and go to step 2.
   * </p>
   * @param fields If "fields" set to "*", the request will return full details of the job.
   * If "fields" is missing, will only return the job ID. Currently the value can only
   * be "*", other values are not allowed and will throw exception.
   * @param showall If "showall" is set to "true", the request will return all jobs the user
   * has permission to view, not only the jobs belonging to the user.
   * @param jobid If "jobid" is present, the records whose Job Id is lexicographically greater
   * than "jobid" are only returned. For example, if "jobid" = "job_201312091733_0001",
   * the jobs whose Job ID is greater than "job_201312091733_0001" are returned. The number of
   * records returned depends on the value of "numrecords".
   * @param numrecords If the "jobid" and "numrecords" parameters are present, the top #numrecords
   * records appearing after "jobid" will be returned after sorting the Job Id list
   * lexicographically.
   * If "jobid" parameter is missing and "numrecords" is present, the top #numrecords will
   * be returned after lexicographically sorting the Job Id list. If "jobid" parameter is present
   * and "numrecords" is missing, all the records whose Job Id is greater than "jobid" are returned.
   * @return list of job items based on the filter conditions specified by the user.
   */
  @GET
  @Path("jobs")
  @Produces({MediaType.APPLICATION_JSON})
  public List<JobItemBean> showJobList(@QueryParam("fields") String fields,
                                       @QueryParam("showall") boolean showall,
                                       @QueryParam("jobid") String jobid,
                                       @QueryParam("numrecords") String numrecords)
    throws NotAuthorizedException, BadParam, IOException, InterruptedException {

    verifyUser();

    boolean showDetails = false;
    if (fields!=null && !fields.equals("*")) {
      throw new BadParam("fields value other than * is not supported");
    }
    if (fields!=null && fields.equals("*")) {
      showDetails = true;
    }

    ListDelegator ld = new ListDelegator(appConf);
    List<String> list = ld.run(getDoAsUser(), showall);
    List<JobItemBean> detailList = new ArrayList<JobItemBean>();
    int currRecord = 0;
    int numRecords;

    // Parse numrecords to an integer
    try {
      if (numrecords != null) {
        numRecords = Integer.parseInt(numrecords);
  if (numRecords <= 0) {
    throw new BadParam("numrecords should be an integer > 0");
  }
      }
      else {
        numRecords = -1;
      }
    }
    catch(Exception e) {
      throw new BadParam("Invalid numrecords format: numrecords should be an integer > 0");
    }

    // Sort the list as requested
    boolean isAscendingOrder = true;
    switch (appConf.getListJobsOrder()) {
    case lexicographicaldesc:
      Collections.sort(list, Collections.reverseOrder());
      isAscendingOrder = false;
      break;
    case lexicographicalasc:
    default:
      Collections.sort(list);
      break;
    }

    for (String job : list) {
      // If numRecords = -1, fetch all records.
      // Hence skip all the below checks when numRecords = -1.
      if (numRecords != -1) {
        // If currRecord >= numRecords, we have already fetched the top #numRecords
        if (currRecord >= numRecords) {
          break;
        }
        else if (jobid == null || jobid.trim().length() == 0) {
            currRecord++;
        }
        // If the current record needs to be returned based on the
        // filter conditions specified by the user, increment the counter
        else if (isAscendingOrder && job.compareTo(jobid) > 0 || !isAscendingOrder && job.compareTo(jobid) < 0) {
          currRecord++;
        }
        // The current record should not be included in the output detailList.
        else {
          continue;
        }
      }
      JobItemBean jobItem = new JobItemBean();
      jobItem.id = job;
      if (showDetails) {
        StatusDelegator sd = new StatusDelegator(appConf);
        try {
          jobItem.detail = sd.run(getDoAsUser(), job);
        }
        catch(Exception ex) {
          /*if we could not get status for some reason, log it, and send empty status back with
          * just the ID so that caller knows to even look in the log file*/
          LOG.info("Failed to get status detail for jobId='" + job + "'", ex);
          jobItem.detail = new QueueStatusBean(job, "Failed to retrieve status; see WebHCat logs");
        }
      }
      detailList.add(jobItem);
    }
    return detailList;
  }

  /**
   * Notify on a completed job.  Called by JobTracker.
   */
  @GET
  @Path("internal/complete/{jobid}")
  @Produces({MediaType.APPLICATION_JSON})
  public CompleteBean completeJob(@PathParam("jobid") String jobid,
                                  @QueryParam("status") String jobStatus)
    throws CallbackFailedException, IOException {
    LOG.debug("Received callback " + theUriInfo.getRequestUri());
    CompleteDelegator d = new CompleteDelegator(appConf);
    return d.run(jobid, jobStatus);
  }

  /**
   * Verify that we have a valid user.  Throw an exception if invalid.
   */
  public void verifyUser() throws NotAuthorizedException {
    String requestingUser = getRequestingUser();
    if (requestingUser == null) {
      String msg = "No user found.";
      if (!UserGroupInformation.isSecurityEnabled()) {
        msg += "  Missing " + PseudoAuthenticator.USER_NAME + " parameter.";
      }
      throw new NotAuthorizedException(msg);
    }
    if(doAs != null && !doAs.equals(requestingUser)) {
      /*if doAs user is different than logged in user, need to check that
      that logged in user is authorized to run as 'doAs'*/
      ProxyUserSupport.validate(requestingUser, getRequestingHost(requestingUser, request), doAs);
    }
  }

  /**
   * All 'tasks' spawned by WebHCat should be run as this user.  W/o doAs query parameter
   * this is just the user making the request (or
   * {@link org.apache.hadoop.security.authentication.client.PseudoAuthenticator#USER_NAME}
   * query param).
   * @return value of doAs query parameter or {@link #getRequestingUser()}
   */
  private String getDoAsUser() {
    return doAs != null && !doAs.equals(getRequestingUser()) ? doAs : getRequestingUser();
  }
  /**
   * Verify that the parameter exists.  Throw an exception if invalid.
   */
  public void verifyParam(String param, String name)
    throws BadParam {
    if (param == null) {
      throw new BadParam("Missing " + name + " parameter");
    }
  }

  /**
   * Verify that the parameter exists.  Throw an exception if invalid.
   */
  public void verifyParam(List<String> param, String name)
    throws BadParam {
    if (param == null || param.isEmpty()) {
      throw new BadParam("Missing " + name + " parameter");
    }
  }

  public static final Pattern DDL_ID = Pattern.compile("[a-zA-Z]\\w*");

  /**
   * Verify that the parameter exists and is a simple DDL identifier
   * name.  Throw an exception if invalid.
   *
   * Bug: This needs to allow for quoted ddl identifiers.
   */
  public void verifyDdlParam(String param, String name)
    throws BadParam {
    verifyParam(param, name);
    Matcher m = DDL_ID.matcher(param);
    if (!m.matches()) {
      throw new BadParam("Invalid DDL identifier " + name);
    }
  }
  /**
   * Get the user name from the security context, i.e. the user making the HTTP request.
   * With simple/pseudo security mode this should return the
   * value of user.name query param, in kerberos mode it's the kinit'ed user.
   */
  private String getRequestingUser() {
    if (theSecurityContext == null) {
      return null;
    }
    String userName = null;
    if (theSecurityContext.getUserPrincipal() == null) {
      userName = Main.UserNameHandler.getUserName(request);
    }
    else {
      userName = theSecurityContext.getUserPrincipal().getName();
    }
    if(userName == null) {
      return null;
    }
    //map hue/foo.bar@something.com->hue since user group checks
    // and config files are in terms of short name
    return UserGroupInformation.createRemoteUser(userName).getShortUserName();
  }

  /**
   * The callback url on this server when a task is completed.
   */
  public String getCompletedUrl() {
    if (theUriInfo == null) {
      return null;
    }
    if (theUriInfo.getBaseUri() == null) {
      return null;
    }
    return theUriInfo.getBaseUri() + VERSION
      + "/internal/complete/$jobId?status=$jobStatus";
  }

  /**
   * Returns canonical host name from which the request is made; used for doAs validation
   */
  private static String getRequestingHost(String requestingUser, HttpServletRequest request) {
    final String unkHost = "???";
    if(request == null) {
      LOG.warn("request is null; cannot determine hostname");
      return unkHost;
    }
    try {
      String address = request.getRemoteAddr();//returns IP addr
      if(address == null) {
        LOG.warn(MessageFormat.format("Request remote address is NULL for user [{0}]", requestingUser));
        return unkHost;
      }

      //Inet4Address/Inet6Address
      String hostName = InetAddress.getByName(address).getCanonicalHostName();
      if(LOG.isDebugEnabled()) {
        LOG.debug(MessageFormat.format("Resolved remote hostname: [{0}]", hostName));
      }
      return hostName;

    } catch (UnknownHostException ex) {
      LOG.warn(MessageFormat.format("Request remote address could not be resolved, {0}", ex.toString(), ex));
      return unkHost;
    }
  }

  private void checkEnableLogPrerequisite(boolean enablelog, String statusdir) throws BadParam {
    if (enablelog && !TempletonUtils.isset(statusdir))
      throw new BadParam("enablelog is only applicable when statusdir is set");
    if(enablelog && "0.23".equalsIgnoreCase(ShimLoader.getMajorVersion())) {
      throw new BadParam("enablelog=true is only supported with Hadoop 1.x");
    }
  }
}
