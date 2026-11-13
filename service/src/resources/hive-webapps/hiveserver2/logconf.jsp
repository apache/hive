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
<%@ page contentType="text/html;charset=UTF-8" %>

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
    <script src="/static/js/logconf.js?v=28184-2"></script>
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
            <h2>Set new logging rules</h2>

            <p id="logconf-error" class="text-danger" style="display: none;"></p>

            <form>
                <div style="display: flex; align-items: flex-end; flex-wrap: wrap; gap: 12px;">
                    <div class="form-group" style="margin: 0;">
                        <label for="logger-name" style="display: block; margin-bottom: 4px;">Logger</label>
                        <select id="logger-name" class="form-control" style="min-width: 320px;"></select>
                    </div>
                    <div class="form-group" style="margin: 0;">
                        <label for="log-level" style="display: block; margin-bottom: 4px;">Level</label>
                        <select id="log-level" class="form-control" style="min-width: 120px;">
                            <option value="TRACE">TRACE</option>
                            <option value="DEBUG">DEBUG</option>
                            <option value="INFO">INFO</option>
                            <option value="WARN">WARN</option>
                            <option value="ERROR">ERROR</option>
                            <option value="FATAL">FATAL</option>
                        </select>
                    </div>
                    <button id="log-level-submit" type="button" class="btn btn-primary">Submit</button>
                </div>
            </form>
        </div>
    </div>

</body>
</html>
