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
package org.apache.hcatalog.templeton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.PseudoAuthenticator;
import org.apache.hcatalog.templeton.tool.TempletonUtils;

/**
 * The Templeton Web API server.
 */
@Path("/v1")
public class Server {
    public static final String VERSION = "v1";

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

    private static final Log LOG = LogFactory.getLog(Server.class);

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
        return d.run(getUser(), exec, false, group, permissions);
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
        if (!TempletonUtils.isset(tablePattern))
            tablePattern = "*";
        return d.listTables(getUser(), db, tablePattern);
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
        return d.createTable(getUser(), db, desc);
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
        return d.createTableLike(getUser(), db, desc);
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
        if ("extended".equals(format))
            return d.descExtendedTable(getUser(), db, table);
        else
            return d.descTable(getUser(), db, table, false);
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
        return d.dropTable(getUser(), db, table, ifExists, group, permissions);
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
        return d.renameTable(getUser(), db, oldTable, newTable, group, permissions);
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
        return d.descTableProperty(getUser(), db, table, property);
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
        return d.listTableProperties(getUser(), db, table);
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
        return d.addOneTableProperty(getUser(), db, table, desc);
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
        return d.listPartitions(getUser(), db, table);
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
        return d.descOnePartition(getUser(), db, table, partition);
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
        return d.addOnePartition(getUser(), db, table, desc);
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
        return d.dropPartition(getUser(), db, table, partition, ifExists,
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
        if (!TempletonUtils.isset(dbPattern))
            dbPattern = "*";
        return d.listDatabases(getUser(), dbPattern);
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
        return d.descDatabase(getUser(), db, "extended".equals(format));
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
        return d.createDatabase(getUser(), desc);
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
        if (TempletonUtils.isset(option))
            verifyDdlParam(option, "option");
        HcatDelegator d = new HcatDelegator(appConf, execService);
        return d.dropDatabase(getUser(), db, ifExists, option,
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
        return d.listColumns(getUser(), db, table);
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
        return d.descOneColumn(getUser(), db, table, column);
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
        return d.addOneColumn(getUser(), db, table, desc);
    }

    /**
     * Run a MapReduce Streaming job.
     */
    @POST
    @Path("mapreduce/streaming")
    @Produces({MediaType.APPLICATION_JSON})
    public EnqueueBean mapReduceStreaming(@FormParam("input") List<String> inputs,
                                          @FormParam("output") String output,
                                          @FormParam("mapper") String mapper,
                                          @FormParam("reducer") String reducer,
                                          @FormParam("file") List<String> files,
                                          @FormParam("define") List<String> defines,
                                          @FormParam("cmdenv") List<String> cmdenvs,
                                          @FormParam("arg") List<String> args,
                                          @FormParam("statusdir") String statusdir,
                                          @FormParam("callback") String callback)
        throws NotAuthorizedException, BusyException, BadParam, QueueException,
        ExecuteException, IOException, InterruptedException {
        verifyUser();
        verifyParam(inputs, "input");
        verifyParam(mapper, "mapper");
        verifyParam(reducer, "reducer");

        StreamingDelegator d = new StreamingDelegator(appConf);
        return d.run(getUser(), inputs, output, mapper, reducer,
            files, defines, cmdenvs, args,
            statusdir, callback, getCompletedUrl());
    }

    /**
     * Run a MapReduce Jar job.
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
                                    @FormParam("callback") String callback)
        throws NotAuthorizedException, BusyException, BadParam, QueueException,
        ExecuteException, IOException, InterruptedException {
        verifyUser();
        verifyParam(jar, "jar");
        verifyParam(mainClass, "class");

        JarDelegator d = new JarDelegator(appConf);
        return d.run(getUser(),
            jar, mainClass,
            libjars, files, args, defines,
            statusdir, callback, getCompletedUrl());
    }

    /**
     * Run a Pig job.
     */
    @POST
    @Path("pig")
    @Produces({MediaType.APPLICATION_JSON})
    public EnqueueBean pig(@FormParam("execute") String execute,
                           @FormParam("file") String srcFile,
                           @FormParam("arg") List<String> pigArgs,
                           @FormParam("files") String otherFiles,
                           @FormParam("statusdir") String statusdir,
                           @FormParam("callback") String callback)
        throws NotAuthorizedException, BusyException, BadParam, QueueException,
        ExecuteException, IOException, InterruptedException {
        verifyUser();
        if (execute == null && srcFile == null)
            throw new BadParam("Either execute or file parameter required");

        PigDelegator d = new PigDelegator(appConf);
        return d.run(getUser(),
            execute, srcFile,
            pigArgs, otherFiles,
            statusdir, callback, getCompletedUrl());
    }

    /**
     * Run a Hive job.
     */
    @POST
    @Path("hive")
    @Produces({MediaType.APPLICATION_JSON})
    public EnqueueBean hive(@FormParam("execute") String execute,
                            @FormParam("file") String srcFile,
                            @FormParam("define") List<String> defines,
                            @FormParam("statusdir") String statusdir,
                            @FormParam("callback") String callback)
        throws NotAuthorizedException, BusyException, BadParam, QueueException,
        ExecuteException, IOException, InterruptedException {
        verifyUser();
        if (execute == null && srcFile == null)
            throw new BadParam("Either execute or file parameter required");

        HiveDelegator d = new HiveDelegator(appConf);
        return d.run(getUser(), execute, srcFile, defines,
            statusdir, callback, getCompletedUrl());
    }

    /**
     * Return the status of the jobid.
     */
    @GET
    @Path("queue/{jobid}")
    @Produces({MediaType.APPLICATION_JSON})
    public QueueStatusBean showQueueId(@PathParam("jobid") String jobid)
        throws NotAuthorizedException, BadParam, IOException, InterruptedException {
        
        verifyUser();
        verifyParam(jobid, ":jobid");

        StatusDelegator d = new StatusDelegator(appConf);
        return d.run(getUser(), jobid);
    }

    /**
     * Kill a job in the queue.
     */
    @DELETE
    @Path("queue/{jobid}")
    @Produces({MediaType.APPLICATION_JSON})
    public QueueStatusBean deleteQueueId(@PathParam("jobid") String jobid)
        throws NotAuthorizedException, BadParam, IOException, InterruptedException {
        
        verifyUser();
        verifyParam(jobid, ":jobid");

        DeleteDelegator d = new DeleteDelegator(appConf);
        return d.run(getUser(), jobid);
    }

    /**
     * Return all the known job ids for this user.
     */
    @GET
    @Path("queue")
    @Produces({MediaType.APPLICATION_JSON})
    public List<String> showQueueList()
        throws NotAuthorizedException, BadParam, IOException, InterruptedException {
        
        verifyUser();

        ListDelegator d = new ListDelegator(appConf);
        return d.run(getUser());
    }

    /**
     * Notify on a completed job.
     */
    @GET
    @Path("internal/complete/{jobid}")
    @Produces({MediaType.APPLICATION_JSON})
    public CompleteBean completeJob(@PathParam("jobid") String jobid)
        throws CallbackFailedException, IOException {
        CompleteDelegator d = new CompleteDelegator(appConf);
        return d.run(jobid);
    }

    /**
     * Verify that we have a valid user.  Throw an exception if invalid.
     */
    public void verifyUser()
        throws NotAuthorizedException {
        if (getUser() == null) {
            String msg = "No user found.";
            if (!UserGroupInformation.isSecurityEnabled())
                msg += "  Missing " + PseudoAuthenticator.USER_NAME + " parameter.";
            throw new NotAuthorizedException(msg);
        }
    }

    /**
     * Verify that the parameter exists.  Throw an exception if invalid.
     */
    public void verifyParam(String param, String name)
        throws BadParam {
        if (param == null)
            throw new BadParam("Missing " + name + " parameter");
    }

    /**
     * Verify that the parameter exists.  Throw an exception if invalid.
     */
    public void verifyParam(List<String> param, String name)
        throws BadParam {
        if (param == null || param.isEmpty())
            throw new BadParam("Missing " + name + " parameter");
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
        if (!m.matches())
            throw new BadParam("Invalid DDL identifier " + name);
    }

    /**
     * Get the user name from the security context.
     */
    public String getUser() {
        if (theSecurityContext == null)
            return null;
        if (theSecurityContext.getUserPrincipal() == null)
            return null;
        return theSecurityContext.getUserPrincipal().getName();
    }

    /**
     * The callback url on this server when a task is completed.
     */
    public String getCompletedUrl() {
        if (theUriInfo == null)
            return null;
        if (theUriInfo.getBaseUri() == null)
            return null;
        return theUriInfo.getBaseUri() + VERSION
            + "/internal/complete/$jobId";
    }
}
