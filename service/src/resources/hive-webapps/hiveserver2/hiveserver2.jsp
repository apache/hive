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
  import="org.apache.hive.http.HttpServer"
  import="org.apache.hadoop.hive.ql.QueryInfo"
  import="org.apache.hive.service.cli.session.SessionManager"
  import="org.apache.hive.service.cli.session.HiveSession"
  import="javax.servlet.ServletContext"
  import="java.util.ArrayList"
  import="java.util.Date"
  import="java.util.List"
  import="jodd.net.HtmlEncoder"
%>
<%@ include file="ui-common.jspf" %>
<%
ServletContext ctx = getServletContext();
Configuration conf = (Configuration) ctx.getAttribute("hive.conf");
long startcode = conf.getLong("startcode", System.currentTimeMillis());
SessionManager sessionManager = (SessionManager) ctx.getAttribute("hive.sm");
String remoteUser = request.getRemoteUser();
long now = System.currentTimeMillis();

List<HiveSession> sessions = new ArrayList<HiveSession>();
List<QueryInfo> open = new ArrayList<QueryInfo>();
List<QueryInfo> closed = new ArrayList<QueryInfo>();
if (sessionManager != null) {
  for (HiveSession s : sessionManager.getSessions())
    if (HttpServer.hasAccess(remoteUser, s.getUserName(), ctx, request)) sessions.add(s);
  for (QueryInfo q : sessionManager.getOperationManager().getLiveQueryInfos())
    if (HttpServer.hasAccess(remoteUser, q.getUserName(), ctx, request)) open.add(q);
  for (QueryInfo q : sessionManager.getOperationManager().getHistoricalQueryInfos())
    if (HttpServer.hasAccess(remoteUser, q.getUserName(), ctx, request)) closed.add(q);
}
// Finished queries: most recently closed first (in-progress entries last).
closed.sort((a, b) -> Long.compare(
    b.getEndTime() == null ? Long.MIN_VALUE : b.getEndTime(),
    a.getEndTime() == null ? Long.MIN_VALUE : a.getEndTime()));
// Query metrics by OperationState, counted across live + historical queries.
int pendingQ = 0, runningQ = 0, closedQ = 0, errorQ = 0;
List<QueryInfo> allQ = new ArrayList<QueryInfo>(open);
allQ.addAll(closed);
for (QueryInfo q : allQ) {
  String st = String.valueOf(q.getState());
  if ("PENDING".equals(st)) pendingQ++;
  else if ("RUNNING".equals(st)) runningQ++;
  else if ("CLOSED".equals(st)) closedQ++;
  else if ("ERROR".equals(st)) errorQ++;
}
// Live queries split into pending (queued) and active (everything else live).
List<QueryInfo> pendingList = new ArrayList<QueryInfo>();
List<QueryInfo> activeList = new ArrayList<QueryInfo>();
for (QueryInfo q : open) {
  if ("PENDING".equals(String.valueOf(q.getState()))) pendingList.add(q);
  else activeList.add(q);
}
long up = (now - startcode) / 1000;
String uptime = (up / 86400) + "d " + ((up % 86400) / 3600) + "h " + ((up % 3600) / 60) + "m";

String rev = HiveVersionInfo.getRevision();
if (rev != null && rev.length() > 8) rev = rev.substring(0, 8);
String host; try { host = java.net.InetAddress.getLocalHost().getHostName(); } catch (Exception e) { host = "unknown"; }
String javaInfo = System.getProperty("java.version") + " &middot; " + System.getProperty("java.vm.name");
String osInfo = System.getProperty("os.name") + " " + System.getProperty("os.version");
String engine = conf.get("hive.execution.engine", "?");
String msUris = conf.get("hive.metastore.uris", "");
String metastore = (msUris == null || msUris.trim().isEmpty()) ? "embedded" : msUris;
int cores = Runtime.getRuntime().availableProcessors();
%>
<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <title>Dashboard &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("dashboard", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Dashboard</h1>
        <p class="text-xs text-slate-400">Live overview of this HiveServer2 instance</p>
      </div>
      <%= topbarTools() %>
    </header>
    <main class="p-6 w-full max-w-[1400px] stagger">
      <div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-6">
        <%= kpi("Active sessions", String.valueOf(sessions.size()), "connected clients", true) %>
        <%= kpi("Pending queries", String.valueOf(pendingQ), "queued", "text-blue-600 dark:text-blue-400", null) %>
        <%= kpi("Active queries", String.valueOf(runningQ), "running now", "text-cyan-600 dark:text-cyan-300", null) %>
        <%= kpi("Finished queries", String.valueOf(closedQ), "completed", "text-emerald-600 dark:text-emerald-400", "Queries in CLOSED state (client finished; results released).") %>
        <%= kpi("Failed queries", String.valueOf(errorQ), "errored", "text-red-600 dark:text-red-400", null) %>
        <%= kpi("Uptime", uptime, "since " + new Date(startcode), false) %>
      </div>

      <%= panelOpen("Active sessions", "sessions", sessions.size()) %>
        <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
          <th class="px-5 py-2.5 font-medium">User</th><th class="px-5 py-2.5 font-medium">IP address</th>
          <th class="px-5 py-2.5 font-medium text-right">Operations</th><th class="px-5 py-2.5 font-medium text-right">Active (s)</th>
          <th class="px-5 py-2.5 font-medium text-right">Idle (s)</th>
        </tr></thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
<% for (HiveSession s : sessions) { %>
          <tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40">
            <td class="px-5 py-2.5 font-mono"><%= HtmlEncoder.text(s.getUserName()) %></td>
            <td class="px-5 py-2.5 font-mono text-slate-500 dark:text-slate-400"><%= HtmlEncoder.text(s.getIpAddress()) %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= s.getOpenOperationCount() %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= (now - s.getCreationTime()) / 1000 %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= (now - s.getLastAccessTime()) / 1000 %></td>
          </tr>
<% } if (sessions.isEmpty()) { %>
          <tr><td colspan="5" class="px-5 py-8 text-center text-slate-400">No active sessions</td></tr>
<% } %>
        </tbody></table></div></section>

      <%= panelOpen("Pending queries", "pending", pendingList.size()) %>
        <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
          <th class="px-5 py-2.5 font-medium">User</th><th class="px-5 py-2.5 font-medium">Query</th>
          <th class="px-5 py-2.5 font-medium">Engine</th>
          <th class="px-5 py-2.5 font-medium">Submitted</th><th class="px-5 py-2.5 font-medium text-right">Waiting (s)</th><th class="px-5 py-2.5"></th>
        </tr></thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
<% for (QueryInfo q : pendingList) {
     String qs = q.getQueryDisplay() == null ? "Unknown" : q.getQueryDisplay().getQueryString(); %>
          <tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40">
            <td class="px-5 py-2.5 font-mono"><%= HtmlEncoder.text(q.getUserName()) %></td>
            <td class="px-5 py-2.5"><span class="block font-mono text-xs max-w-[420px] truncate text-slate-600 dark:text-slate-300" title="<%= HtmlEncoder.text(qs) %>"><%= HtmlEncoder.text(qs) %></span></td>
            <td class="px-5 py-2.5 font-mono text-slate-500"><%= HtmlEncoder.text(String.valueOf(q.getExecutionEngine())) %></td>
            <td class="px-5 py-2.5 font-mono text-xs text-slate-500"><%= new Date(q.getBeginTime()) %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= q.getElapsedTime() / 1000 %></td>
            <td class="px-5 py-2.5"><a class="text-brand-600 dark:text-brand hover:underline" href="/query_page.html?operationId=<%= q.getOperationId() %>">Details</a></td>
          </tr>
<% } if (pendingList.isEmpty()) { %>
          <tr><td colspan="6" class="px-5 py-8 text-center text-slate-400">No pending queries</td></tr>
<% } %>
        </tbody></table></div><div data-pager="pending" data-page-size="25"></div></section>

      <%= panelOpen("Active queries", "open", activeList.size()) %>
        <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
          <th class="px-5 py-2.5 font-medium">User</th><th class="px-5 py-2.5 font-medium">Query</th>
          <th class="px-5 py-2.5 font-medium">Engine</th><th class="px-5 py-2.5 font-medium">State</th>
          <th class="px-5 py-2.5 font-medium">Opened</th><th class="px-5 py-2.5 font-medium text-right">Elapsed (s)</th><th class="px-5 py-2.5"></th>
        </tr></thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
<% for (QueryInfo q : activeList) {
     String qs = q.getQueryDisplay() == null ? "Unknown" : q.getQueryDisplay().getQueryString();
     String st = String.valueOf(q.getState()); %>
          <tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40">
            <td class="px-5 py-2.5 font-mono"><%= HtmlEncoder.text(q.getUserName()) %></td>
            <td class="px-5 py-2.5"><span class="block font-mono text-xs max-w-[420px] truncate text-slate-600 dark:text-slate-300" title="<%= HtmlEncoder.text(qs) %>"><%= HtmlEncoder.text(qs) %></span></td>
            <td class="px-5 py-2.5 font-mono text-slate-500"><%= HtmlEncoder.text(String.valueOf(q.getExecutionEngine())) %></td>
            <td class="px-5 py-2.5"><span class="<%= pill(st) %>"><%= HtmlEncoder.text(st) %></span></td>
            <td class="px-5 py-2.5 font-mono text-xs text-slate-500"><%= new Date(q.getBeginTime()) %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= q.getElapsedTime() / 1000 %></td>
            <td class="px-5 py-2.5"><a class="text-brand-600 dark:text-brand hover:underline" href="/query_page.html?operationId=<%= q.getOperationId() %>">Details</a></td>
          </tr>
<% } if (activeList.isEmpty()) { %>
          <tr><td colspan="7" class="px-5 py-8 text-center text-slate-400">No queries currently executing</td></tr>
<% } %>
        </tbody></table></div><div data-pager="open" data-page-size="25"></div></section>

      <%= panelOpen("Finished queries", "closed", closed.size()) %>
        <thead><tr class="text-left text-[11px] uppercase tracking-wide text-slate-400 border-b border-slate-200 dark:border-slate-800">
          <th class="px-5 py-2.5 font-medium">User</th><th class="px-5 py-2.5 font-medium">Query</th>
          <th class="px-5 py-2.5 font-medium">Engine</th><th class="px-5 py-2.5 font-medium">State</th>
          <th class="px-5 py-2.5 font-medium">Closed</th><th class="px-5 py-2.5 font-medium text-right">Latency (s)</th><th class="px-5 py-2.5"></th>
        </tr></thead>
        <tbody class="divide-y divide-slate-100 dark:divide-slate-800">
<% for (QueryInfo q : closed) {
     String qs = q.getQueryDisplay() == null ? "Unknown" : q.getQueryDisplay().getQueryString();
     String st = String.valueOf(q.getState()); %>
          <tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40">
            <td class="px-5 py-2.5 font-mono"><%= HtmlEncoder.text(q.getUserName()) %></td>
            <td class="px-5 py-2.5"><span class="block font-mono text-xs max-w-[420px] truncate text-slate-600 dark:text-slate-300" title="<%= HtmlEncoder.text(qs) %>"><%= HtmlEncoder.text(qs) %></span></td>
            <td class="px-5 py-2.5 font-mono text-slate-500"><%= HtmlEncoder.text(String.valueOf(q.getExecutionEngine())) %></td>
            <td class="px-5 py-2.5"><span class="<%= pill(st) %>"><%= HtmlEncoder.text(st) %></span></td>
            <td class="px-5 py-2.5 font-mono text-xs text-slate-500"><%= q.getEndTime() == null ? "In progress" : new Date(q.getEndTime()) %></td>
            <td class="px-5 py-2.5 text-right tabular-nums font-mono text-slate-500"><%= q.getRuntime() == null ? "n/a" : (q.getRuntime() / 1000) %></td>
            <td class="px-5 py-2.5"><a class="text-brand-600 dark:text-brand hover:underline" href="/query_page.html?operationId=<%= q.getOperationId() %>">Details</a></td>
          </tr>
<% } if (closed.isEmpty()) { %>
          <tr><td colspan="7" class="px-5 py-8 text-center text-slate-400">No finished queries yet</td></tr>
<% } %>
        </tbody></table></div><div data-pager="closed" data-page-size="25"></div></section>

      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 mb-6 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">System information</h2></div>
        <div class="p-5 grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
          <%= attrCard("Hive version", HiveVersionInfo.getVersion() + " (r" + rev + ")") %>
          <%= attrCard("Built", HiveVersionInfo.getDate()) %>
          <%= attrCard("Built by", HiveVersionInfo.getUser()) %>
          <%= attrCard("HS2 started", new Date(startcode).toString()) %>
          <%= attrCard("Uptime", uptime) %>
          <%= attrCard("Host", HtmlEncoder.text(host)) %>
          <%= attrCard("Java", javaInfo) %>
          <%= attrCard("Operating system", osInfo) %>
          <%= attrCard("CPU cores", String.valueOf(cores)) %>
          <%= attrCard("Execution engine", HtmlEncoder.text(engine)) %>
          <%= attrCard("Metastore", HtmlEncoder.text(metastore)) %>
        </div>
      </section>
    </main>
  </div>
</div>
</body>
</html>
