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
         import="org.apache.hive.http.HttpServer"
         import="org.apache.hive.service.cli.operation.Operation"
         import="org.apache.hive.service.cli.operation.SQLOperation"
         import="org.apache.hadoop.hive.ql.QueryInfo"
         import="org.apache.hive.service.cli.session.SessionManager"
         import="org.apache.hive.service.cli.session.HiveSession"
         import="javax.servlet.ServletContext"
         import="java.util.Collection"
         import="java.util.Date"
         import="java.util.List"
         import="jodd.net.HtmlEncoder"
%>

<%
    ServletContext ctx = getServletContext();
    Configuration conf = (Configuration)ctx.getAttribute("hive.conf");
    long startcode = conf.getLong("startcode", System.currentTimeMillis());
    SessionManager sessionManager =
            (SessionManager)ctx.getAttribute("hive.sm");
    String remoteUser = request.getRemoteUser();
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

    <link rel="stylesheet" type="text/css" href="/static/css/json.human.css">
    <script src="/static/js/jquery.min.js"></script>
    <script src="/static/js/json.human.js"></script>
    <script src="/static/js/logconf.js"></script>
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
                    <li><a href="/logconf.jsp">Configure logging</a></li>
                </ul>
            </div><!--/.nav-collapse -->
        </div>
    </div>


    <div class="container">
        <div class="row inner_header">
            <div class="page-header">
                <h2>Configure HiveServer2 logging</h2>
            </div>
        </div>
        <div class="row">

            <div id="current-logs-container">
                <table id="current-logs-table" class="table">
                    <thead>
                        <tr>
                            <th>Logger name</th>
                            <th>Log level</th>
                        </tr>
                    </thead>
                    <tbody id="current-logs">

                    </tbody>
                </table>
            </div>
            <% Collection<HiveSession> hiveSessions = sessionManager.getSessions();
            for (HiveSession hiveSession: hiveSessions) {
            if( hiveSessions.size() > 0 && HttpServer.hasAccess(remoteUser, hiveSession.getUserName(), ctx, request) ) { %>
            <h2>Set new logging rules</h2>

            <form class="form-inline">
                <div class="form-group">
                    <input type="text" id="logger-name" class="form-control" placeholder="Logger name">
                </div>
                <div class="form-group">
                    <select id="log-level" class="form-control">
                        <option value="TRACE">TRACE</option>
                        <option value="DEBUG">DEBUG</option>
                        <option value="INFO">INFO</option>
                        <option value="WARN">WARN</option>
                        <option value="ERROR">ERROR</option>
                        <option value="FATAL">FATAL</option>
                    </select>
                </div>

                <button id="log-level-submit" type="button" class="btn btn-primary">Submit</button>
            </form>
            <% } else {%>
                <p>Cannot configure logging rules unless user <%= hiveSession.getUserName() %> has admin privileges</p>
            <% }
             } %>
        </div>
    </div>

</body>
</html>
