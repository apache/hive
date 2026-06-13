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
  <title>Stack traces &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
  <script src="/static/js/stacks.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("stacks", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Stack traces</h1>
        <p class="text-xs text-slate-400">Live JVM thread dump &middot; <b id="stk-total" class="text-slate-500">0</b> threads</p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <svg id="stk-live" class="w-4 h-4 text-slate-400 hidden" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-3-6.7L21 8"/><path d="M21 3v5h-5"/></svg>
        <button id="stkRaw" class="inline-flex items-center h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">Raw dump</button>
        <button id="stkReload" class="inline-flex items-center gap-2 h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">&#10227; Reload</button>
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1400px]">
      <div id="stk-err" class="hidden mb-4 rounded-lg border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-500/10 text-red-700 dark:text-red-400 px-4 py-2 text-sm">Could not load /stacks.</div>
      <div id="stk-filters" class="flex flex-wrap items-center gap-3 mb-5 text-sm text-slate-500">
        <span id="stk-states" class="text-xs"></span>
        <div class="ml-auto flex items-center gap-3">
          <span class="text-xs"><b id="stk-shown" class="text-slate-900 dark:text-slate-200">0</b> shown</span>
          <select id="stkState" class="h-9 px-3 rounded-lg text-sm bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
            <option value="">All states</option><option value="RUNNABLE">RUNNABLE</option><option value="WAITING">WAITING</option>
            <option value="TIMED_WAITING">TIMED_WAITING</option><option value="BLOCKED">BLOCKED</option>
          </select>
          <input id="stkSearch" placeholder="Search thread or frame&hellip;" class="h-9 w-72 px-3 rounded-lg text-sm bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
        </div>
      </div>
      <div id="stk-list" class="space-y-2">
        <div class="text-sm text-slate-400">Loading thread dump&hellip;</div>
      </div>
      <pre id="stk-raw" class="hidden m-0 p-4 text-xs font-mono leading-relaxed overflow-auto text-slate-600 dark:text-slate-300 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl max-h-[75vh] whitespace-pre-wrap"></pre>
    </main>
  </div>
</div>
</body>
</html>
