/*
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
package org.apache.hive.service.servlet;

/**
 * Shared chrome for the modernized HiveServer2 Web UI (sidebar, status pills,
 * theme boot). A single source of truth so the JSP pages (via ui-common.jspf)
 * and the Jamon-rendered query page render an identical console layout.
 */
public final class WebUiLayout {
  private WebUiLayout() {
  }

  /** Inline script that applies the persisted theme before first paint. */
  public static String themeBoot() {
    return "(function(){try{var t=localStorage.getItem('hive.ui.theme');"
        + "if(t==='light')document.documentElement.classList.remove('dark');"
        + "else document.documentElement.classList.add('dark');}catch(e){}})();";
  }

  /** Tailwind classes for a status pill colored by the given state. */
  public static String pill(String s) {
    String base = "inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-semibold ";
    String u = s == null ? "" : s.toUpperCase();
    if (u.contains("FINISH") || "CLOSED".equals(u) || "OK".equals(u)) {
      return base + "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-400";
    }
    if (u.contains("ERROR") || u.contains("FAIL")) {
      return base + "bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-400";
    }
    if (u.contains("RUN")) {
      return base + "bg-cyan-100 text-cyan-700 dark:bg-cyan-500/15 dark:text-cyan-300";
    }
    if (u.contains("CANCEL") || u.contains("TIMEOUT") || u.contains("TIMED")) {
      return base + "bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-400";
    }
    if (u.contains("PENDING") || u.contains("INITIAL") || u.contains("QUEUE")) {
      return base + "bg-blue-100 text-blue-700 dark:bg-blue-500/15 dark:text-blue-300";
    }
    return base + "bg-slate-100 text-slate-600 dark:bg-slate-700/40 dark:text-slate-300";
  }

  private static String groupLabel(String t) {
    return "<div class=\"px-3 mb-1 text-[10px] font-semibold uppercase tracking-wider "
        + "text-slate-400 dark:text-slate-500\">" + t + "</div>";
  }

  private static String navItem(String href, String label, String svg, boolean active) {
    String base = "flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ";
    String cls = active
        ? base + "bg-brand/10 text-brand-700 dark:text-brand font-medium"
        : base + "text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800 "
            + "hover:text-slate-900 dark:hover:text-slate-100";
    return "<a href=\"" + href + "\" class=\"" + cls + "\">"
        + "<svg class=\"w-[18px] h-[18px] shrink-0\" viewBox=\"0 0 24 24\" fill=\"none\" "
        + "stroke=\"currentColor\" stroke-width=\"1.7\" stroke-linecap=\"round\" "
        + "stroke-linejoin=\"round\">" + svg + "</svg>"
        + "<span>" + label + "</span></a>";
  }

  /** The full left sidebar; {@code active} is the current page key. */
  public static String sidebar(String active, String version, String uptime) {
    if (active == null) {
      active = "";
    }
    String iDash = "<rect x='3' y='3' width='7' height='7' rx='1'/>"
        + "<rect x='14' y='3' width='7' height='7' rx='1'/>"
        + "<rect x='3' y='14' width='7' height='7' rx='1'/>"
        + "<rect x='14' y='14' width='7' height='7' rx='1'/>";
    String iCfg = "<path d='M4 7h10M18 7h2M4 12h2M10 12h10M4 17h7M15 17h5'/>"
        + "<circle cx='16' cy='7' r='2'/><circle cx='8' cy='12' r='2'/>"
        + "<circle cx='13' cy='17' r='2'/>";
    String iMetrics = "<path d='M3 21h18'/>"
        + "<rect x='5' y='10' width='3' height='8' rx='1'/>"
        + "<rect x='11' y='5' width='3' height='13' rx='1'/>"
        + "<rect x='17' y='13' width='3' height='5' rx='1'/>";
    String iStacks = "<path d='M12 3l9 5-9 5-9-5 9-5z'/><path d='M3 13l9 5 9-5'/>";
    String iLogs = "<path d='M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z'/>"
        + "<path d='M14 3v5h5M9 13h6M9 17h6'/>";
    String iLlap = "<rect x='3' y='4' width='18' height='6' rx='1'/>"
        + "<rect x='3' y='14' width='18' height='6' rx='1'/>"
        + "<path d='M7 7h.01M7 17h.01'/>";
    String iLog = "<circle cx='12' cy='12' r='3'/>"
        + "<path d='M12 2v3M12 19v3M2 12h3M19 12h3M5 5l1.5 1.5M17.5 17.5L19 19"
        + "M19 5l-1.5 1.5M6.5 17.5L5 19'/>";
    String foot = (uptime == null || uptime.isEmpty())
        ? "Hive " + version
        : "Hive " + version + " &middot; up " + uptime;
    return "<aside class=\"w-60 shrink-0 flex flex-col border-r border-slate-200 "
        + "dark:border-slate-800 bg-white dark:bg-slate-900\">"
        + "<div class=\"h-14 flex items-center gap-2.5 px-5 border-b border-slate-200 "
        + "dark:border-slate-800\">"
        + "<img src=\"/static/hive-logo.png\" alt=\"Hive\" class=\"shrink-0\" "
        + "style=\"height:24px;width:auto;display:block\">"
        + "<span class=\"font-semibold tracking-tight text-slate-900 "
        + "dark:text-slate-100 whitespace-nowrap\">Hive WebUI</span></div>"
        + "<nav class=\"flex-1 overflow-y-auto p-3 space-y-6\">"
        + "<div class=\"space-y-0.5\">" + groupLabel("Overview")
        + navItem("/hiveserver2.jsp", "Dashboard", iDash, "dashboard".equals(active))
        + navItem("/metrics.jsp", "Metrics", iMetrics, "metrics".equals(active))
        + navItem("/conf.jsp", "Configuration", iCfg, "config".equals(active)) + "</div>"
        + "<div class=\"space-y-0.5\">" + groupLabel("Diagnostics")
        + navItem("/stacks.jsp", "Stack traces", iStacks, "stacks".equals(active))
        + navItem("/logs.jsp", "Logs", iLogs, "logs".equals(active))
        + navItem("/logconf.jsp", "Logging", iLog, "logging".equals(active))
        + navItem("/llap.jsp", "LLAP daemons", iLlap, "llap".equals(active)) + "</div></nav>"
        + "<div class=\"p-4 border-t border-slate-200 dark:border-slate-800 text-xs text-slate-500\">"
        + "<div class=\"flex items-center gap-2\">"
        + "<span class=\"w-2 h-2 rounded-full bg-emerald-500 pulse-dot\"></span>Running</div>"
        + "<div class=\"mt-1\">" + foot + "</div></div></aside>";
  }
}
