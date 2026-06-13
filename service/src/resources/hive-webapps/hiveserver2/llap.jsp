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
  import="org.apache.hive.common.util.HiveVersionInfo"
  import="javax.servlet.ServletContext"
%>
<%@ include file="ui-common.jspf" %>
<%
ServletContext ctx = getServletContext();
Configuration conf = (Configuration) ctx.getAttribute("hive.conf");
long startcode = conf != null ? conf.getLong("startcode", System.currentTimeMillis()) : System.currentTimeMillis();
long up = (System.currentTimeMillis() - startcode) / 1000;
String uptime = (up / 86400) + "d " + ((up % 86400) / 3600) + "h " + ((up % 3600) / 60) + "m";
%>
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <title>LLAP &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <link rel="stylesheet" type="text/css" href="/static/css/json.human.css">
  <script src="/static/js/jquery.min.js?v=<%= startcode %>"></script>
  <script src="/static/js/json.human.js?v=<%= startcode %>"></script>
  <script src="/static/js/llap.js?v=<%= startcode %>"></script>
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("llap", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">LLAP daemons</h1>
        <p class="text-xs text-slate-400">Live Long and Process daemon cluster status</p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1400px] stagger">
      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">Daemons</h2></div>
        <div class="p-5 text-sm leading-relaxed [&_table]:w-full [&_th]:text-left [&_th]:px-3 [&_th]:py-2 [&_td]:px-3 [&_td]:py-2 [&_table]:border-collapse">
          <div id="show-data" class="text-slate-600 dark:text-slate-300">
            <div class="text-slate-400">Loading LLAP daemon status&hellip; If no daemons appear, LLAP may not be enabled on this instance.</div>
          </div>
        </div>
      </section>
    </main>
  </div>
</div>
</body>
</html>
