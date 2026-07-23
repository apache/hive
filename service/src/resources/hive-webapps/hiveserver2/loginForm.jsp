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
  import="jodd.net.HtmlEncoder"
%>
<%
  String status = (String) request.getAttribute(LoginServlet.LOGIN_FAILURE_MESSAGE);
%>
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <meta name="description" content="Sign in to the Hive Web UI">
  <title>Sign in &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script>(function(){try{var t=localStorage.getItem('hive.ui.theme');if(t==='light')document.documentElement.classList.remove('dark');else document.documentElement.classList.add('dark');}catch(e){}})();</script>
  <link href="/static/css/hive.tw.css?v=<%= System.currentTimeMillis() %>" rel="stylesheet">
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased min-h-screen flex items-center justify-center p-4">
  <div class="w-full max-w-sm">
    <div class="flex items-center justify-center gap-2.5 mb-6">
      <img src="/static/hive-logo.png" alt="Hive" style="height:30px;width:auto;display:block">
      <span class="text-lg font-semibold tracking-tight text-slate-900 dark:text-slate-100">Hive WebUI</span>
    </div>
    <form id="login" action="login" method="post" class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-6 space-y-4">
      <h1 class="text-base font-semibold text-center text-slate-900 dark:text-slate-100">Sign in</h1>
<% if (status != null) { %>
      <div class="rounded-lg border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-500/10 text-red-700 dark:text-red-400 px-3 py-2 text-sm"><%= HtmlEncoder.text(status) %></div>
<% } %>
      <div>
        <label for="username" class="block text-xs text-slate-400 mb-1">Username</label>
        <input id="username" name="username" type="text" required placeholder="Username" autocomplete="username"
          class="w-full h-10 px-3 rounded-lg text-sm bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
      </div>
      <div>
        <label for="password" class="block text-xs text-slate-400 mb-1">Password</label>
        <input id="password" name="password" type="password" required placeholder="Password" autocomplete="current-password"
          class="w-full h-10 px-3 rounded-lg text-sm bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
      </div>
      <input id="redirectPath" name="redirectPath" type="hidden">
      <button id="submit" type="submit" class="w-full h-10 rounded-lg text-sm font-medium bg-brand text-slate-900 hover:bg-brand-500 transition-colors">Sign in</button>
    </form>
    <p class="mt-4 text-center text-xs text-slate-400">Apache Hive</p>
  </div>
</body>
</html>
