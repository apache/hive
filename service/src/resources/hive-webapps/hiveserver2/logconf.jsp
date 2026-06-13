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
  <title>Logging &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/jquery.min.js?v=<%= startcode %>"></script>
  <script src="/static/js/logconf.js?v=<%= startcode %>"></script>
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("logging", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Logging</h1>
        <p class="text-xs text-slate-400">Inspect and adjust runtime log levels</p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <button onclick="loadLoggers()" class="inline-flex items-center gap-2 h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">&#10227; Reload</button>
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1100px] stagger">
      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 mb-6 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">Current loggers</h2></div>
        <div class="overflow-x-auto"><table id="current-logs-table" class="w-full text-sm">
          <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
            <th class="px-6 py-3 font-medium">Logger name</th><th class="px-6 py-3 font-medium w-40">Level</th>
          </tr></thead>
          <tbody id="current-logs" class="divide-y divide-slate-100 dark:divide-slate-800"></tbody>
        </table></div>
      </section>

      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">Set log level</h2></div>
        <div class="p-5 flex flex-wrap items-end gap-3">
          <div>
            <label class="block text-xs text-slate-400 mb-1">Logger name</label>
            <select id="logger-name" class="h-9 w-80 px-3 rounded-lg text-sm bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
              <option value="">Loading loggers&hellip;</option>
            </select>
          </div>
          <div>
            <label class="block text-xs text-slate-400 mb-1">Level</label>
            <select id="log-level" class="h-9 px-3 rounded-lg text-sm bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
              <option value="TRACE">TRACE</option><option value="DEBUG">DEBUG</option>
              <option value="INFO">INFO</option><option value="WARN">WARN</option>
              <option value="ERROR">ERROR</option><option value="FATAL">FATAL</option>
            </select>
          </div>
          <button id="log-level-submit" type="button" class="h-9 px-4 rounded-lg text-sm font-medium bg-brand text-slate-900 hover:bg-brand-500 transition-colors">Apply</button>
        </div>
        <p class="px-5 pb-5 -mt-2 text-xs text-slate-400">Requires administrator privileges; changes take effect immediately and are not persisted across restarts.</p>
      </section>
    </main>
  </div>
</div>
</body>
</html>
