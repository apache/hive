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
  <title>Metrics &middot; Hive WebUI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <link rel="icon" type="image/png" href="/static/favicon.png">
  <script><%= THEME_BOOT %></script>
  <link href="/static/css/hive.tw.css?v=<%= startcode %>" rel="stylesheet">
  <script src="/static/js/hive-ui.js?v=<%= startcode %>"></script>
  <script src="/static/js/metrics.js?v=<%= startcode %>"></script>
</head>
<body class="font-sans bg-slate-50 dark:bg-slate-950 text-slate-700 dark:text-slate-300 antialiased">
<div class="flex min-h-screen">
  <%= sidebar("metrics", HiveVersionInfo.getVersion(), uptime) %>
  <div class="flex-1 min-w-0 flex flex-col">
    <header class="sticky top-0 z-10 h-14 flex items-center gap-4 px-6 border-b border-slate-200 dark:border-slate-800 bg-white/80 dark:bg-slate-900/80 backdrop-blur">
      <div>
        <h1 class="text-base font-semibold leading-tight text-slate-900 dark:text-slate-100">Metrics</h1>
        <p class="text-xs text-slate-400">Live JVM &amp; process metrics &middot; <span id="metrics-ts">loading&hellip;</span></p>
      </div>
      <div class="ml-auto flex items-center gap-2">
        <span class="inline-flex items-center gap-2 h-8 px-3 rounded-lg text-sm text-emerald-600 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-900/60">
          <svg id="metrics-live" class="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-3-6.7L21 8"/><path d="M21 3v5h-5"/></svg>
          Live &middot; 5s
        </span>
        <a href="/jmx" class="inline-flex items-center h-8 px-3 rounded-lg text-sm border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100">Raw /jmx</a>
        <button onclick="toggleTheme()" aria-label="Toggle theme" class="inline-flex items-center justify-center w-8 h-8 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-500 hover:text-slate-900 dark:hover:text-slate-100"><span id="themeIcon">&#9728;</span></button>
      </div>
    </header>
    <main class="p-6 w-full max-w-[1400px] stagger">
      <div id="metrics-err" class="hidden mb-4 rounded-lg border border-red-200 dark:border-red-900/60 bg-red-50 dark:bg-red-500/10 text-red-700 dark:text-red-400 px-4 py-2 text-sm">Could not load /jmx &mdash; metrics endpoint unavailable or access denied.</div>

      <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-5 flex items-center gap-5">
          <div class="relative w-32 h-32 shrink-0">
            <svg viewBox="0 0 120 120" class="w-32 h-32 -rotate-90">
              <circle cx="60" cy="60" r="52" fill="none" stroke-width="10" class="stroke-slate-200 dark:stroke-slate-800"/>
              <circle id="heap-ring" cx="60" cy="60" r="52" fill="none" stroke-width="10" stroke-linecap="round" style="stroke-dasharray:326.7;stroke-dashoffset:326.7;transition:stroke-dashoffset .8s cubic-bezier(.2,.7,.2,1),stroke .4s"/>
            </svg>
            <div class="absolute inset-0 flex items-center justify-center"><span id="heap-pct" class="text-2xl font-semibold tabular-nums text-slate-900 dark:text-slate-100">&ndash;</span></div>
          </div>
          <div>
            <div class="text-sm font-semibold text-slate-900 dark:text-slate-100">Heap memory</div>
            <div id="heap-detail" class="mt-1 text-xs text-slate-400 font-mono">&ndash;</div>
            <div class="mt-2 text-xs text-slate-400">Non-heap used: <span id="nonheap-val" class="font-mono text-slate-500">&ndash;</span></div>
          </div>
        </div>
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-5 flex items-center gap-5">
          <div class="relative w-32 h-32 shrink-0">
            <svg viewBox="0 0 120 120" class="w-32 h-32 -rotate-90">
              <circle cx="60" cy="60" r="52" fill="none" stroke-width="10" class="stroke-slate-200 dark:stroke-slate-800"/>
              <circle id="cpu-ring" cx="60" cy="60" r="52" fill="none" stroke-width="10" stroke-linecap="round" style="stroke-dasharray:326.7;stroke-dashoffset:326.7;transition:stroke-dashoffset .8s cubic-bezier(.2,.7,.2,1),stroke .4s"/>
            </svg>
            <div class="absolute inset-0 flex items-center justify-center"><span id="cpu-pct" class="text-2xl font-semibold tabular-nums text-slate-900 dark:text-slate-100">&ndash;</span></div>
          </div>
          <div>
            <div class="text-sm font-semibold text-slate-900 dark:text-slate-100">Process CPU</div>
            <div id="cpu-detail" class="mt-1 text-xs text-slate-400 font-mono">&ndash;</div>
            <div class="mt-2 text-xs text-slate-400">JVM uptime: <span id="jvm-uptime" class="font-mono text-slate-500">&ndash;</span></div>
          </div>
        </div>
      </div>

      <div class="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4"><div class="text-xs font-medium uppercase tracking-wide text-slate-400">Threads</div><div id="threads-val" class="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-100">&ndash;</div><div id="threads-sub" class="mt-1 text-xs text-slate-400">&nbsp;</div></div>
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4"><div class="text-xs font-medium uppercase tracking-wide text-slate-400">Loaded classes</div><div id="classes-val" class="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-100">&ndash;</div><div id="classes-sub" class="mt-1 text-xs text-slate-400">&nbsp;</div></div>
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4"><div class="text-xs font-medium uppercase tracking-wide text-slate-400">GC collections</div><div id="gc-count" class="mt-1 text-3xl font-semibold tabular-nums text-slate-900 dark:text-slate-100">&ndash;</div><div class="mt-1 text-xs text-slate-400">across all collectors</div></div>
        <div class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 p-4"><div class="text-xs font-medium uppercase tracking-wide text-slate-400">GC time</div><div id="gc-time" class="mt-1 text-3xl font-semibold tabular-nums text-brand-600 dark:text-brand">&ndash;</div><div class="mt-1 text-xs text-slate-400">total pause time</div></div>
      </div>

      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 mb-6 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">Heap usage over time</h2><span class="text-xs text-slate-400">last ~5 min (live)</span></div>
        <div class="p-5">
          <svg viewBox="0 0 600 90" preserveAspectRatio="none" class="w-full h-24">
            <polyline id="heap-spark-area" fill="rgb(242 177 52 / .12)" stroke="none" points=""></polyline>
            <polyline id="heap-spark-line" fill="none" stroke="#f2b134" stroke-width="2" vector-effect="non-scaling-stroke" points=""></polyline>
          </svg>
        </div>
      </section>

      <section class="rounded-xl border border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900 overflow-hidden">
        <div class="flex items-center gap-3 px-5 h-12 border-b border-slate-200 dark:border-slate-800"><h2 class="text-sm font-semibold text-slate-900 dark:text-slate-100">Garbage collectors</h2><span class="text-xs text-slate-400">bar = share of total pause time</span></div>
        <div id="gc-cards" class="p-5 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
          <div class="text-sm text-slate-400">Loading&hellip;</div>
        </div>
      </section>
    </main>
  </div>
</div>
</body>
</html>
