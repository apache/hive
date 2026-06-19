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
  import="org.apache.hive.service.servlet.LoginServlet"
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="description" content="Login - Hive">
        <title>HiveServer2 - WebUI</title>
        <link rel="icon" href="/static/favicon.ico">
        <link href="/static/css/bootstrap.min.css" rel="stylesheet">
        <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
        <link href="/static/css/hive.css" rel="stylesheet">
        <link href="/static/css/login.css" rel="stylesheet">
    </head>
    <body>
        <div class="login-form">
            <form id="login" action="login" method="post">
                <h2 class="text-center">Log in</h2>
                <div class="form-group">
                    <input id="username" name="username" type="text" class="form-control" placeholder="Username" required="required">
                </div>
                <div class="form-group">
                    <input id="password" name="password" type="password" class="form-control" placeholder="Password" required="required">
                </div>
                 <%
                        String status = (String)request.getAttribute(LoginServlet.LOGIN_FAILURE_MESSAGE);
                        if(status != null) {
                 %>
                          <p style="color:red;"><%= status %></p>
                 <%
                        }
                 %>
                <input id="redirectPath" name="redirectPath" type="hidden">
                <div class="form-group">
                    <button id="submit" type="submit" class="btn btn-primary btn-block">Log In</button>
                </div>
            </form>
        </div>
    </body>
</html>