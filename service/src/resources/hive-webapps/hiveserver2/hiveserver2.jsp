<%--
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
--%>
<%@ page contentType="text/html;charset=UTF-8"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hive.conf.HiveConf"
  import="org.apache.hadoop.hive.conf.HiveConf.ConfVars"
  import="org.apache.hive.common.util.HiveVersionInfo"
  import="org.apache.hive.service.cli.operation.Operation"
  import="org.apache.hive.service.cli.operation.SQLOperation"
  import="org.apache.hive.service.cli.operation.SQLOperationDisplay"
  import="org.apache.hive.service.cli.session.SessionManager"
  import="org.apache.hive.service.cli.session.HiveSession"
  import="javax.servlet.ServletContext"
  import="java.util.Collection"
  import="java.util.Date"
  import="java.util.List"
  import="jodd.util.HtmlEncoder"
%>

<%
ServletContext ctx = getServletContext();
Configuration conf = (Configuration)ctx.getAttribute("hive.conf");
long startcode = conf.getLong("startcode", System.currentTimeMillis());
SessionManager sessionManager =
  (SessionManager)ctx.getAttribute("hive.sm");
%>

<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>HiveServer2</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">

    <link href="/static/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
    <link href="/static/css/hive.css" rel="stylesheet">
  </head>

  <body>
  <div class="navbar  navbar-fixed-top navbar-default">
      <div class="container">
          <div class="navbar-header">
              <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                  <span class="icon-bar"></span>
                  <span class="icon-bar"></span>
                  <span class="icon-bar"></span>
              </button>
              <a class="navbar-brand" href="/hiveserver2.jsp"><img src="/static/hive_logo.jpeg" alt="Hive Logo"/></a>
          </div>
          <div class="collapse navbar-collapse">
              <ul class="nav navbar-nav">
                <li class="active"><a href="/">Home</a></li>
                <li><a href="/logs/">Local logs</a></li>
                <li><a href="/jmx">Metrics Dump</a></li>
                <li><a href="/conf">Hive Configuration</a></li>
                <li><a href="/stacks">Stack Trace</a></li>
                <li><a href="/llap.html">Llap Daemons</a></li>
            </ul>
          </div><!--/.nav-collapse -->
        </div>
      </div>
    </div>

<div class="container">
    <div class="row inner_header">
        <div class="page-header">
            <h1>HiveServer2</h1>
        </div>
    </div>
    <div class="row">

<%
if (sessionManager != null) { 
  long currentTime = System.currentTimeMillis();
%> 

<section>
<h2>Active Sessions</h2>
<table id="attributes_table" class="table table-striped">
    <tr>
        <th>User Name</th>
        <th>IP Address</th>
        <th>Operation Count</th>
        <th>Active Time (s)</th>
        <th>Idle Time (s)</th>
    </tr>
<%
Collection<HiveSession> hiveSessions = sessionManager.getSessions();
for (HiveSession hiveSession: hiveSessions) {
%>
    <tr>
        <td><%= hiveSession.getUserName() %></td>
        <td><%= hiveSession.getIpAddress() %></td>
        <td><%= hiveSession.getOpenOperationCount() %></td>
        <td><%= (currentTime - hiveSession.getCreationTime())/1000 %></td>
        <td><%= (currentTime - hiveSession.getLastAccessTime())/1000 %></td>
    </tr>
<%
}
%>
<tr>
  <td colspan="5">Total number of sessions: <%= hiveSessions.size() %></td>
</tr>
</table>
</section>

<section>
<h2>Open Queries</h2>
<table id="attributes_table" class="table table-striped">
    <tr>
        <th>User Name</th>
        <th>Query</th>
        <th>Execution Engine</th>
        <th>State</th>
        <th>Opened Timestamp</th>
        <th>Opened (s)</th>
        <th>Latency (s)</th>
        <th>Drilldown Link</th>
    </tr>
    <%
      int queries = 0;
      Collection<SQLOperationDisplay> operations = sessionManager.getOperationManager().getLiveSqlOperations();
      for (SQLOperationDisplay operation : operations) {
          queries++;
    %>
    <tr>
        <td><%= operation.getUserName() %></td>
        <td><%= HtmlEncoder.strict(operation.getQueryDisplay() == null ? "Unknown" : operation.getQueryDisplay().getQueryString()) %></td>
        <td><%= operation.getExecutionEngine() %>
        <td><%= operation.getState() %></td>
        <td><%= new Date(operation.getBeginTime()) %></td>
        <td><%= operation.getElapsedTime()/1000 %></td>
        <td><%= operation.getRuntime() == null ? "Not finished" : operation.getRuntime()/1000 %></td>
        <% String link = "/query_page?operationId=" + operation.getOperationId(); %>
        <td>  <a href= <%= link %>>Drilldown</a> </td>
    </tr>

<%
  }
%>
<tr>
  <td colspan="8">Total number of queries: <%= queries %></td>
</tr>
</table>
</section>


<section>
<h2>Last Max <%= conf.get(ConfVars.HIVE_SERVER2_WEBUI_MAX_HISTORIC_QUERIES.varname) %> Closed Queries</h2>
<table id="attributes_table" class="table table-striped">
    <tr>
        <th>User Name</th>
        <th>Query</th>
        <th>Execution Engine</th>
        <th>State</th>
        <th>Opened (s)</th>
        <th>Closed Timestamp</th>
        <th>Latency (s)</th>
        <th>Drilldown Link</th>
    </tr>
    <%
      queries = 0;
      operations = sessionManager.getOperationManager().getHistoricalSQLOperations();
      for (SQLOperationDisplay operation : operations) {
          queries++;
    %>
    <tr>
        <td><%= operation.getUserName() %></td>
        <td><%= HtmlEncoder.strict(operation.getQueryDisplay() == null ? "Unknown" : operation.getQueryDisplay().getQueryString()) %></td>
        <td><%= operation.getExecutionEngine() %>
        <td><%= operation.getState() %></td>
        <td><%= operation.getElapsedTime()/1000 %></td>
        <td><%= operation.getEndTime() == null ? "In Progress" : new Date(operation.getEndTime()) %></td>
        <td><%= operation.getRuntime() == null ? "n/a" : operation.getRuntime()/1000 %></td>
        <% String link = "/query_page?operationId=" + operation.getOperationId(); %>
        <td>  <a href= <%= link %>>Drilldown</a> </td>
    </tr>

<%
  }
%>
<tr>
  <td colspan="8">Total number of queries: <%= queries %></td>
</tr>
</table>
</section>

<%
 }
%>

    <section>
    <h2>Software Attributes</h2>
    <table id="attributes_table" class="table table-striped">
        <tr>
            <th>Attribute Name</th>
            <th>Value</th>
            <th>Description</th>
        </tr>
        <tr>
            <td>Hive Version</td>
            <td><%= HiveVersionInfo.getVersion() %>, r<%= HiveVersionInfo.getRevision() %></td>
            <td>Hive version and revision</td>
        </tr>
        <tr>
            <td>Hive Compiled</td>
            <td><%= HiveVersionInfo.getDate() %>, <%= HiveVersionInfo.getUser() %></td>
            <td>When Hive was compiled and by whom</td>
        </tr>
        <tr>
            <td>HiveServer2 Start Time</td>
            <td><%= new Date(startcode) %></td>
            <td>Date stamp of when this HiveServer2 was started</td>
        </tr>
    </table>
    </section>
    </div>
</div>
</body>
</html>
