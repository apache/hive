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
  <title>Logs &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
  <script src="/static/js/logs.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("logs", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Logs</h1>
        <p class="text-xs text-slate-400">Browse and tail server log files</p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <a href="/logs/" class="inline-flex items-center h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">Raw /logs</a>
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1500px]">
      <div class="grid grid-cols-1 lg:grid-cols-[240px_1fr] gap-4">
        <aside class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-2 h-max">
          <div class="px-2 py-1 text-[10px] font-semibold uppercase tracking-wider text-slate-400">Log files</div>
          <div id="log-files" class="space-y-0.5"><div class="px-3 py-2 text-xs text-slate-400">Loading&hellip;</div></div>
        </aside>
        <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 overflow-hidden flex flex-col min-w-0">
          <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800">
            <h2 id="log-name" class="text-sm font-semibold font-mono text-slate-900 dark:text-slate-100 truncate">&ndash;</h2>
            <span id="log-trunc" class="hidden text-[11px] text-amber-600 dark:text-amber-400 whitespace-nowrap">last 1000 lines</span>
            <div class="ml-auto flex items-center gap-2">
              <input id="logSearch" placeholder="Filter lines&hellip;" class="h-8 w-48 px-3 rounded-lg text-sm bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
              <a id="logDownload" href="#" download title="Download full file" aria-label="Download" class="inline-flex items-center justify-center w-8 h-8 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v12"/><path d="M7 10l5 5 5-5"/><path d="M5 21h14"/></svg></a>
              <button id="logFollow" class="inline-flex items-center gap-2 h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><svg class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14"/><path d="M19 12l-7 7-7-7"/></svg><span>Follow</span></button>
              <button id="logReload" class="inline-flex items-center justify-center w-8 h-8 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">&#10227;</button>
            </div>
          </div>
          <pre id="log-content" class="m-0 p-4 text-xs font-mono leading-relaxed overflow-auto text-slate-600 dark:text-slate-300 bg-slate-50 dark:bg-slate-950 max-h-[72vh] whitespace-pre-wrap break-all">Select a log file&hellip;</pre>
        </section>
      </div>
    </main>
  </div>
</div>
</body>
</html>
