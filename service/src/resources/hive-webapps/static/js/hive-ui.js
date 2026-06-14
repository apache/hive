/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * hive-ui.js - progressive enhancements for the HiveServer2 Web UI: dark/light
 * theme (Tailwind 'dark' class strategy), per-table search, optional
 * auto-refresh, and the config explorer filter. All optional: with JS disabled
 * the server-rendered tables still work.
 */
(function () {
  "use strict";
  var KEY = "hive.ui.theme";

  window.toggleTheme = function () {
    var root = document.documentElement;
    var dark = root.classList.toggle("dark");
    try { localStorage.setItem(KEY, dark ? "dark" : "light"); } catch (e) {}
    syncThemeLabel(dark);
  };
  function syncThemeLabel(dark) {
    var b = document.getElementById("themeIcon");
    if (b) b.textContent = dark ? "\u2600" : "\u263e";
  }

  // ---- toast notifications ----
  window.hiveToast = function (msg, type) {
    var c = document.getElementById("hive-toasts");
    if (!c) {
      c = document.createElement("div");
      c.id = "hive-toasts";
      c.style.cssText = "position:fixed;right:16px;bottom:16px;z-index:1000;display:flex;flex-direction:column;gap:8px";
      document.body.appendChild(c);
    }
    var color = type === "err"
      ? "bg-red-50 dark:bg-red-500/10 border-red-200 dark:border-red-900/60 text-red-700 dark:text-red-400"
      : type === "ok"
        ? "bg-emerald-50 dark:bg-emerald-500/10 border-emerald-200 dark:border-emerald-900/60 text-emerald-700 dark:text-emerald-400"
        : "bg-white dark:bg-slate-800 border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-200";
    var t = document.createElement("div");
    t.className = "rounded-lg border px-4 py-2.5 text-sm shadow-lg transition-opacity duration-300 " + color;
    t.style.opacity = "0";
    t.textContent = msg;
    c.appendChild(t);
    requestAnimationFrame(function () { t.style.opacity = "1"; });
    setTimeout(function () { t.style.opacity = "0"; setTimeout(function () { if (t.parentNode) t.parentNode.removeChild(t); }, 300); }, 3200);
  };

  function tableSearch() {
    document.querySelectorAll("input[data-search]").forEach(function (inp) {
      var id = inp.getAttribute("data-search");
      // Paginated tables run their own filter+paging in pagedTables().
      if (document.querySelector('[data-pager="' + id + '"]')) return;
      inp.addEventListener("input", function () {
        var q = inp.value.toLowerCase();
        var tbl = document.getElementById(id);
        if (!tbl) return;
        var shown = 0;
        tbl.querySelectorAll("tbody tr").forEach(function (tr) {
          var hit = tr.textContent.toLowerCase().indexOf(q) !== -1;
          tr.style.display = hit ? "" : "none";
          if (hit) shown++;
        });
        var c = document.getElementById(id + "-count");
        if (c) c.textContent = shown;
      });
    });
  }

  // Client-side pagination for large tables (e.g. Active/Finished queries):
  // shows PAGE_SIZE rows at a time with Prev/Next, and cooperates with the
  // table's search box so filtering re-pages the matching rows.
  function pagedTables() {
    document.querySelectorAll("[data-pager]").forEach(function (pager) {
      var id = pager.getAttribute("data-pager");
      var size = parseInt(pager.getAttribute("data-page-size") || "25", 10);
      var tbl = document.getElementById(id);
      if (!tbl) return;
      var search = document.querySelector('input[data-search="' + id + '"]');
      var page = 0;

      function dataRows() {
        return Array.prototype.slice.call(tbl.querySelectorAll("tbody tr")).filter(function (tr) {
          return !tr.querySelector("td[colspan]"); // skip the empty-state row
        });
      }

      function render() {
        var q = search ? search.value.toLowerCase() : "";
        var rows = dataRows();
        var matching = rows.filter(function (tr) {
          return tr.textContent.toLowerCase().indexOf(q) !== -1;
        });
        var pages = Math.max(1, Math.ceil(matching.length / size));
        if (page >= pages) page = pages - 1;
        if (page < 0) page = 0;
        rows.forEach(function (tr) { tr.style.display = "none"; });
        var from = page * size;
        matching.slice(from, from + size).forEach(function (tr) { tr.style.display = ""; });
        var cnt = document.getElementById(id + "-count");
        if (cnt) cnt.textContent = matching.length;
        controls(matching.length, pages);
      }

      function controls(total, pages) {
        if (total <= size) { pager.innerHTML = ""; return; }
        var from = page * size + 1;
        var to = Math.min((page + 1) * size, total);
        var btn = "h-7 px-2.5 rounded-md border border-slate-200 dark:border-slate-700 " +
          "hover:bg-slate-100 dark:hover:bg-slate-800 disabled:opacity-40 disabled:pointer-events-none";
        pager.innerHTML =
          '<div class="flex items-center justify-between px-5 py-3 border-t border-slate-200 ' +
          'dark:border-slate-800 text-xs text-slate-500">' +
          "<span>Showing " + from + "\u2013" + to + " of " + total + "</span>" +
          '<div class="flex items-center gap-1">' +
          '<button type="button" data-pg="prev" class="' + btn + '"' + (page === 0 ? " disabled" : "") + '>Prev</button>' +
          '<span class="px-2">Page ' + (page + 1) + " of " + pages + "</span>" +
          '<button type="button" data-pg="next" class="' + btn + '"' + (page >= pages - 1 ? " disabled" : "") + '>Next</button>' +
          "</div></div>";
        var prev = pager.querySelector('[data-pg="prev"]');
        var next = pager.querySelector('[data-pg="next"]');
        if (prev) prev.addEventListener("click", function () { page--; render(); });
        if (next) next.addEventListener("click", function () { page++; render(); });
      }

      if (search) {
        search.addEventListener("input", function () { page = 0; render(); });
      }
      render();
    });
  }

  function autoRefresh() {
    var btn = document.getElementById("refreshBtn");
    if (!btn) return;
    var KEYR = "hive.ui.refresh", secs = 0, timer = null, left = 0;
    var lbl = document.getElementById("refreshLbl");
    function tick() { left--; if (left <= 0) { location.reload(); return; } if (lbl) lbl.textContent = secs + "s \u00b7 " + left; }
    function start(s) {
      secs = s; left = s;
      if (timer) clearInterval(timer);
      btn.classList.toggle("text-brand", s > 0);
      if (lbl) lbl.textContent = s > 0 ? (s + "s") : "Off";
      if (s > 0) timer = setInterval(tick, 1000);
      try { localStorage.setItem(KEYR, String(s)); } catch (e) {}
    }
    btn.addEventListener("click", function () {
      var seq = [0, 5, 10, 30, 60];
      start(seq[(seq.indexOf(secs) + 1) % seq.length]);
    });
    var saved = 0; try { saved = parseInt(localStorage.getItem(KEYR) || "0", 10) || 0; } catch (e) {}
    start(saved);
  }

  function configFilter() {
    var box = document.getElementById("cfgSearch");
    var only = document.getElementById("cfgModified");
    var tbl = document.getElementById("cfgTable");
    if (!tbl) return;
    function apply() {
      var q = (box ? box.value.toLowerCase() : ""), modOnly = only ? only.checked : false, shown = 0;
      tbl.querySelectorAll("tbody tr").forEach(function (tr) {
        var hit = tr.textContent.toLowerCase().indexOf(q) !== -1;
        if (modOnly && tr.getAttribute("data-modified") !== "1") hit = false;
        tr.style.display = hit ? "" : "none";
        if (hit) shown++;
      });
      var c = document.getElementById("cfgShown"); if (c) c.textContent = shown;
    }
    if (box) box.addEventListener("input", apply);
    if (only) only.addEventListener("change", apply);
    apply();
  }

  function countUp() {
    if (window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    document.querySelectorAll("[data-count]").forEach(function (el) {
      var target = parseInt(el.getAttribute("data-count"), 10);
      if (isNaN(target) || target === 0) return;
      var dur = 600, t0 = null;
      function step(ts) {
        if (!t0) t0 = ts;
        var p = Math.min((ts - t0) / dur, 1);
        el.textContent = Math.round(target * (1 - Math.pow(1 - p, 3)));
        if (p < 1) requestAnimationFrame(step);
      }
      requestAnimationFrame(step);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    syncThemeLabel(document.documentElement.classList.contains("dark"));
    tableSearch(); pagedTables(); autoRefresh(); configFilter(); countUp();
  });
})();
