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
 * metrics.js - polls /jmx and renders lightweight SVG charts (ring gauges,
 * a heap sparkline, GC bars) for the Hive WebUI metrics dashboard. No chart
 * library: hand-drawn SVG, offline-safe.
 */
(function () {
  "use strict";
  var R = 52, C = 2 * Math.PI * R;
  var heapHist = [];

  function el(id) { return document.getElementById(id); }
  function ringColor(p) { return p > 0.9 ? "#f85149" : p > 0.7 ? "#d29922" : "#3fb950"; }
  function fmtBytes(b) {
    if (b == null || b < 0) return "n/a";
    var u = ["B", "KB", "MB", "GB", "TB"], i = 0;
    while (b >= 1024 && i < u.length - 1) { b /= 1024; i++; }
    return b.toFixed(i ? 1 : 0) + " " + u[i];
  }
  function setRing(id, pct) {
    var c = el(id); if (!c) return;
    pct = Math.max(0, Math.min(1, pct || 0));
    c.style.stroke = ringColor(pct);
    c.style.strokeDashoffset = (C * (1 - pct)).toFixed(1);
  }
  function setText(id, v) { var e = el(id); if (e) e.textContent = v; }

  function drawSpark(vals) {
    var svg = el("heap-spark"); if (!svg) return;
    var W = 600, H = 90, n = vals.length;
    if (n < 2) return;
    var pts = vals.map(function (v, i) {
      return (i / (n - 1) * W).toFixed(1) + "," + ((1 - v) * H).toFixed(1);
    }).join(" ");
    el("heap-spark-line").setAttribute("points", pts);
    el("heap-spark-area").setAttribute("points", "0," + H + " " + pts + " " + W + "," + H);
  }

  function load() {
    var live = el("metrics-live"); if (live) live.classList.add("spin-slow");
    fetch("/jmx").then(function (r) { return r.json(); }).then(function (j) {
      var b = {};
      (j.beans || []).forEach(function (x) { b[x.name] = x; });

      var mem = b["java.lang:type=Memory"] || {};
      var heap = mem.HeapMemoryUsage || {};
      var hp = heap.max > 0 ? heap.used / heap.max : 0;
      setRing("heap-ring", hp);
      setText("heap-pct", Math.round(hp * 100) + "%");
      setText("heap-detail", fmtBytes(heap.used) + " / " + fmtBytes(heap.max));
      var nh = mem.NonHeapMemoryUsage || {};
      setText("nonheap-val", fmtBytes(nh.used));

      var os = b["java.lang:type=OperatingSystem"] || {};
      var cpu = (os.ProcessCpuLoad != null && os.ProcessCpuLoad >= 0) ? os.ProcessCpuLoad : (os.SystemCpuLoad || 0);
      setRing("cpu-ring", cpu);
      setText("cpu-pct", Math.round(cpu * 100) + "%");
      setText("cpu-detail", (os.AvailableProcessors || "?") + " cores \u00b7 load " +
        (os.SystemLoadAverage != null && os.SystemLoadAverage >= 0 ? os.SystemLoadAverage.toFixed(2) : "n/a"));

      var th = b["java.lang:type=Threading"] || {};
      setText("threads-val", th.ThreadCount != null ? th.ThreadCount : "\u2014");
      setText("threads-sub", "peak " + (th.PeakThreadCount || "\u2014") + " \u00b7 daemon " + (th.DaemonThreadCount || "\u2014"));

      var cl = b["java.lang:type=ClassLoading"] || {};
      setText("classes-val", cl.LoadedClassCount != null ? cl.LoadedClassCount.toLocaleString() : "\u2014");
      setText("classes-sub", (cl.TotalLoadedClassCount || 0).toLocaleString() + " loaded total");

      var rt = b["java.lang:type=Runtime"] || {};
      if (rt.Uptime != null) {
        var s = Math.floor(rt.Uptime / 1000);
        setText("jvm-uptime", Math.floor(s / 86400) + "d " + Math.floor((s % 86400) / 3600) + "h " + Math.floor((s % 3600) / 60) + "m");
      }

      var gcCount = 0, gcTime = 0, gcs = [];
      (j.beans || []).forEach(function (x) {
        if (x.name && x.name.indexOf("type=GarbageCollector") >= 0) {
          var cnt = x.CollectionCount || 0, t = x.CollectionTime || 0;
          gcCount += cnt; gcTime += t;
          var nm = (x.name.match(/name=([^,]+)/) || [])[1] || x.name;
          gcs.push({ name: nm, count: cnt, time: t });
        }
      });
      setText("gc-count", gcCount.toLocaleString());
      setText("gc-time", (gcTime / 1000).toFixed(1) + "s");
      var maxT = Math.max.apply(null, gcs.map(function (g) { return g.time; }).concat([1]));
      var cards = gcs.map(function (g) {
        var avg = g.count > 0 ? (g.time / g.count).toFixed(1) : "0";
        var pct = Math.round(g.time / maxT * 100);
        return "<div class='rounded-lg border border-slate-200 dark:border-slate-800 p-4'>" +
          "<div class='flex items-center justify-between gap-2'>" +
            "<span class='font-mono text-xs text-slate-700 dark:text-slate-200 truncate' title='" + g.name + "'>" + g.name + "</span>" +
            "<span class='text-[11px] text-slate-400 whitespace-nowrap'>avg " + avg + " ms</span></div>" +
          "<div class='mt-2 flex items-baseline gap-2'>" +
            "<span class='text-2xl font-semibold tabular-nums text-slate-900 dark:text-slate-100'>" + g.count.toLocaleString() + "</span>" +
            "<span class='text-xs text-slate-400'>collections</span></div>" +
          "<div class='mt-0.5 text-xs text-slate-400'>" + g.time.toLocaleString() + " ms total pause</div>" +
          "<div class='mt-3 h-1.5 rounded-full bg-slate-100 dark:bg-slate-800 overflow-hidden'>" +
            "<div class='h-full rounded-full bg-brand transition-all duration-700' style='width:" + pct + "%'></div></div></div>";
      }).join("");
      if (el("gc-cards")) el("gc-cards").innerHTML = cards || "<div class='text-sm text-slate-400'>No GC beans available</div>";

      heapHist.push(hp); if (heapHist.length > 60) heapHist.shift();
      drawSpark(heapHist);

      var err = el("metrics-err"); if (err) err.classList.add("hidden");
      var ts = el("metrics-ts"); if (ts) ts.textContent = "updated just now";
    }).catch(function () {
      var err = el("metrics-err"); if (err) err.classList.remove("hidden");
    }).then(function () {
      setTimeout(function () { if (live) live.classList.remove("spin-slow"); }, 400);
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    load();
    setInterval(load, 5000);
  });
})();
