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
 * logs.js - lists the server's log files (via /logs/) and tails a selected
 * file inside the Hive WebUI, with a line filter and follow mode.
 */
(function () {
  "use strict";
  var TAIL = 1000, curUrl = null, curName = "", raw = "", follow = false, timer = null;
  function esc(s) { return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;"); }
  function el(id) { return document.getElementById(id); }

  function listFiles() {
    fetch("/logs/").then(function (r) { return r.text(); }).then(function (html) {
      var doc = new DOMParser().parseFromString(html, "text/html");
      var seen = {}, files = [];
      [].slice.call(doc.querySelectorAll("a")).forEach(function (a) {
        var href = a.getAttribute("href") || "";
        // skip sort-header links (?C=...), parent link, and sub-directories (trailing /)
        if (!href || href.indexOf("?") >= 0 || href === "../" || href.charAt(href.length - 1) === "/") return;
        var name = href.split("/").pop();
        try { name = decodeURIComponent(name); } catch (e) {}
        if (!name || seen[name]) return;
        seen[name] = 1;
        files.push({ name: name, url: href.charAt(0) === "/" ? href : "/logs/" + href });
      });
      files.sort(function (a, b) {
        var pa = /\.pipeout$/.test(a.name) ? 1 : 0, pb = /\.pipeout$/.test(b.name) ? 1 : 0;
        return pa - pb || a.name.localeCompare(b.name);
      });
      var box = el("log-files");
      box.innerHTML = files.length ? files.map(function (f) {
        return "<button data-url='" + esc(f.url) + "' data-name='" + esc(f.name) + "' title='" + esc(f.name) +
          "' class='log-file w-full text-left px-3 py-2 rounded-lg text-xs font-mono truncate text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-800'>" + esc(f.name) + "</button>";
      }).join("") : "<div class='px-3 py-2 text-xs text-slate-400'>No log files</div>";
      box.querySelectorAll(".log-file").forEach(function (b) {
        b.addEventListener("click", function () { open(b.getAttribute("data-url"), b.getAttribute("data-name")); });
      });
      var pref = files.filter(function (f) { return f.name === "hive.log"; })[0]
        || files.filter(function (f) { return !/\.pipeout$/.test(f.name); })[0] || files[0];
      if (pref) open(pref.url, pref.name);
    }).catch(function () { el("log-files").innerHTML = "<div class='px-3 py-2 text-xs text-red-500'>Could not list /logs/</div>"; });
  }

  function highlight() {
    el("log-files").querySelectorAll(".log-file").forEach(function (b) {
      var on = b.getAttribute("data-url") === curUrl;
      b.classList.toggle("bg-slate-100", on); b.classList.toggle("dark:bg-slate-800", on);
      b.classList.toggle("text-slate-900", on); b.classList.toggle("dark:text-slate-100", on);
    });
  }

  function applyFilter() {
    var pre = el("log-content"); if (!pre) return;
    var q = ((el("logSearch") || {}).value || "").toLowerCase();
    if (!q) { pre.innerHTML = raw; return; }
    pre.innerHTML = raw.split("\n").filter(function (l) { return l.toLowerCase().indexOf(q) >= 0; }).join("\n") || "(no matching lines)";
  }

  function open(url, name) {
    curUrl = url; curName = name || url; highlight();
    el("log-name").textContent = curName;
    var dl = el("logDownload"); if (dl) { dl.href = url; dl.setAttribute("download", curName); }
    fetch(url).then(function (r) { return r.text(); }).then(function (txt) {
      var lines = txt.split("\n");
      el("log-trunc").classList.toggle("hidden", lines.length <= TAIL);
      raw = esc(lines.slice(-TAIL).join("\n"));
      applyFilter();
      var pre = el("log-content"); pre.scrollTop = pre.scrollHeight;
    }).catch(function () { raw = "Could not load " + curName; el("log-content").textContent = raw; });
  }

  document.addEventListener("DOMContentLoaded", function () {
    listFiles();
    var s = el("logSearch"); if (s) s.addEventListener("input", applyFilter);
    var r = el("logReload"); if (r) r.addEventListener("click", function () { if (curUrl) open(curUrl, curName); });
    var f = el("logFollow");
    if (f) f.addEventListener("click", function () {
      follow = !follow; f.classList.toggle("text-brand", follow);
      f.querySelector("span").textContent = follow ? "Following" : "Follow";
      if (timer) clearInterval(timer);
      if (follow) timer = setInterval(function () { if (curUrl) open(curUrl, curName); }, 5000);
    });
  });
})();
