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
  import="org.apache.hadoop.hive.conf.HiveConf.ConfVars"
  import="org.apache.hive.common.util.HiveVersionInfo"
  import="javax.servlet.ServletContext"
  import="java.util.Objects"
  import="jodd.net.HtmlEncoder"
%>
<%@ include file="ui-common.jspf" %>
<%!
  boolean isSecret(String k) {
    String l = k.toLowerCase();
    return l.contains("password") || l.contains("secret") || l.contains("credential");
  }
%>
<%
ServletContext ctx = getServletContext();
Configuration conf = (Configuration) ctx.getAttribute("hive.conf");
long startcode = conf != null ? conf.getLong("startcode", System.currentTimeMillis()) : System.currentTimeMillis();
long up = (System.currentTimeMillis() - startcode) / 1000;
String uptime = (up / 86400) + "d " + ((up % 86400) / 3600) + "h " + ((up % 3600) / 60) + "m";
ConfVars[] vars = ConfVars.values();
int total = vars.length, modified = 0;
for (ConfVars cv : vars) {
  String cur = conf != null ? conf.get(cv.varname) : null;
  if (cur != null && !Objects.equals(cur, cv.getDefaultValue())) modified++;
}
%>
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <title>Configuration &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("config", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Configuration</h1>
        <p class="text-xs text-slate-400">Every HiveConf property &mdash; what it is, its value, and its default</p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <a href="/conf" class="inline-flex items-center h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100" title="Raw Hadoop XML/JSON dump">Raw dump</a>
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1400px]">
      <div class="flex flex-wrap items-center gap-4 mb-5 text-sm text-slate-500">
        <span><b class="text-slate-900 dark:text-slate-200"><%= total %></b> properties</span>
        <span><b class="text-amber-600 dark:text-amber-400"><%= modified %></b> modified</span>
        <span><b class="text-slate-900 dark:text-slate-200" id="cfgShown"><%= total %></b> shown</span>
        <div class="ml-auto flex items-center gap-3">
          <label class="inline-flex items-center gap-2 cursor-pointer select-none"><input type="checkbox" id="cfgModified" class="accent-brand"> Modified only</label>
          <input id="cfgSearch" placeholder="Search key or description&hellip;" class="h-9 w-80 px-3 rounded-lg text-sm bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-700 focus:outline-none focus:ring-2 focus:ring-brand/40">
        </div>
      </div>

      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 overflow-hidden">
        <div class="overflow-x-auto"><table class="w-full text-sm" id="cfgTable">
          <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
            <th class="px-5 py-2.5 font-medium w-[30%]">Property</th>
            <th class="px-5 py-2.5 font-medium w-[16%]">Value</th>
            <th class="px-5 py-2.5 font-medium w-[14%]">Default</th>
            <th class="px-5 py-2.5 font-medium">Description</th>
          </tr></thead>
          <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
<%
for (ConfVars cv : vars) {
  String key = cv.varname;
  String def = cv.getDefaultValue();
  String cur = conf != null ? conf.get(key) : null;
  if (cur == null) cur = def;
  boolean mod = cur != null && !Objects.equals(cur, def);
  boolean secret = isSecret(key);
  String curDisp = secret ? "********" : (cur == null ? "" : cur);
  String defDisp = secret ? "********" : (def == null ? "" : def);
  String desc = cv.getDescription();
%>
            <tr data-modified="<%= mod ? "1" : "0" %>" class="hover:bg-slate-50 dark:hover:bg-slate-800/40 align-top">
              <td class="px-5 py-2.5 font-mono text-xs text-brand-700 dark:text-brand break-all"><%= HtmlEncoder.text(key) %><% if (mod) { %> <span class="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-semibold bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-400">MODIFIED</span><% } %></td>
              <td class="px-5 py-2.5 font-mono text-xs break-all"><%= HtmlEncoder.text(curDisp) %></td>
              <td class="px-5 py-2.5 font-mono text-xs text-slate-400 break-all"><%= HtmlEncoder.text(defDisp) %></td>
              <td class="px-5 py-2.5 text-slate-500 dark:text-slate-400"><%= desc == null ? "" : HtmlEncoder.text(desc) %></td>
            </tr>
<% } %>
          </tbody>
        </table></div>
      </section>
      <p class="mt-4 text-xs text-slate-400">Secrets (password / secret / credential keys) are masked.</p>
    </main>
  </div>
</div>
</body>
</html>
