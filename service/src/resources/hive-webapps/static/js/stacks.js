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
 * stacks.js - fetches the /stacks thread dump, parses it into per-thread
 * cards with state badges, and wires search/filter for the Hive WebUI.
 */
(function () {
  "use strict";
  var rawText = "";
  function esc(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
  function statePill(st) {
    var c = "bg-slate-100 text-slate-600 dark:bg-slate-700/40 dark:text-slate-300";
    if (st === "RUNNABLE") c = "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-400";
    else if (st === "BLOCKED") c = "bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-400";
    else if (st === "WAITING" || st === "TIMED_WAITING") c = "bg-blue-100 text-blue-700 dark:bg-blue-500/15 dark:text-blue-300";
    return "<span class='inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-semibold " + c + "'>" + st + "</span>";
  }

  function parse(text) {
    var lines = text.split("\n"), threads = [], cur = null;
    lines.forEach(function (ln) {
      var m = ln.match(/^Thread (\d+) \((.*)\):\s*$/);
      if (m) { cur = { id: m[1], name: m[2], state: "", body: [] }; threads.push(cur); return; }
      if (!cur) return;
      var sm = ln.match(/^\s+State:\s+(\S+)/);
      if (sm) { cur.state = sm[1]; return; }
      if (/^\s+Stack:\s*$/.test(ln)) return;
      if (ln.trim()) cur.body.push(ln.trim());
    });
    return threads;
  }

  function render(threads) {
    var list = document.getElementById("stk-list");
    if (!list) return;
    var counts = {};
    var html = threads.map(function (t) {
      counts[t.state] = (counts[t.state] || 0) + 1;
      var frames = t.body.length ? esc(t.body.join("\n")) : "(no stack frames)";
      return "<div class='thread rounded-lg border border-slate-200 dark:border-slate-800 overflow-hidden' data-text='" +
        esc((t.name + " " + t.body.join(" ")).toLowerCase()) + "' data-state='" + t.state + "'>" +
        "<div class='thread-head flex items-center gap-3 px-4 py-2.5 cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-800/40'>" +
          statePill(t.state) +
          "<span class='font-mono text-xs text-slate-700 dark:text-slate-200 truncate'>" + esc(t.name) + "</span>" +
          "<span class='ml-auto text-xs text-slate-400 whitespace-nowrap'>#" + t.id + " &middot; " + t.body.length + " frames</span></div>" +
        "<pre class='thread-body hidden m-0 px-4 py-3 text-xs font-mono leading-relaxed overflow-x-auto text-slate-500 dark:text-slate-400 bg-slate-50 dark:bg-slate-950 border-t border-slate-200 dark:border-slate-800'>" + frames + "</pre></div>";
    }).join("");
    list.innerHTML = html || "<div class='text-sm text-slate-400'>No threads</div>";
    document.getElementById("stk-total").textContent = threads.length;
    var sm = document.getElementById("stk-states");
    if (sm) sm.textContent = Object.keys(counts).sort().map(function (k) { return counts[k] + " " + k.toLowerCase(); }).join("  &middot;  ");

    list.querySelectorAll(".thread-head").forEach(function (h) {
      h.addEventListener("click", function () { h.nextElementSibling.classList.toggle("hidden"); });
    });
  }

  function filter() {
    var q = (document.getElementById("stkSearch") || {}).value || "";
    q = q.toLowerCase();
    var st = (document.getElementById("stkState") || {}).value || "";
    var shown = 0;
    document.querySelectorAll("#stk-list .thread").forEach(function (t) {
      var ok = t.getAttribute("data-text").indexOf(q) !== -1 && (!st || t.getAttribute("data-state") === st);
      t.style.display = ok ? "" : "none"; if (ok) shown++;
    });
    var s = document.getElementById("stk-shown"); if (s) s.textContent = shown;
  }

  function load() {
    var live = document.getElementById("stk-live"); if (live) live.classList.add("spin-slow");
    fetch("/stacks").then(function (r) { return r.text(); }).then(function (txt) {
      rawText = txt;
      render(parse(txt)); filter();
      var pre = document.getElementById("stk-raw");
      if (pre && !pre.classList.contains("hidden")) pre.textContent = rawText;
      var err = document.getElementById("stk-err"); if (err) err.classList.add("hidden");
    }).catch(function () {
      var err = document.getElementById("stk-err"); if (err) err.classList.remove("hidden");
    }).then(function () { if (live) setTimeout(function () { live.classList.remove("spin-slow"); }, 400); });
  }

  function toggleRaw() {
    var pre = document.getElementById("stk-raw"), list = document.getElementById("stk-list");
    var filt = document.getElementById("stk-filters"), btn = document.getElementById("stkRaw");
    var show = pre.classList.contains("hidden");
    if (show) { pre.textContent = rawText || "(no data)"; pre.classList.remove("hidden"); list.classList.add("hidden"); if (filt) filt.classList.add("hidden"); if (btn) btn.classList.add("text-brand"); }
    else { pre.classList.add("hidden"); list.classList.remove("hidden"); if (filt) filt.classList.remove("hidden"); if (btn) btn.classList.remove("text-brand"); }
  }

  document.addEventListener("DOMContentLoaded", function () {
    load();
    var b = document.getElementById("stkSearch"); if (b) b.addEventListener("input", filter);
    var s = document.getElementById("stkState"); if (s) s.addEventListener("change", filter);
    var r = document.getElementById("stkReload"); if (r) r.addEventListener("click", load);
    var rw = document.getElementById("stkRaw"); if (rw) rw.addEventListener("click", toggleRaw);
  });
  window.stkLoad = load;
})();
