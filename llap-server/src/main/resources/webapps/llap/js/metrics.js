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

$(function() {
  // JMX json keys
  const heapUsedKey = "MemHeapUsedM";
  const heapMaxKey = "MemHeapMaxM";

  const cacheUsedKey = "CacheCapacityUsed";
  const cacheMaxKey = "CacheCapacityTotal";
  const cacheHitKey = "CacheHitRatio";
  const cacheRequestKey = "CacheReadRequests";

  const numExecutorsKey = "NumExecutors";
  const executorUsedKey = "ExecutorsStatus";
  const numQueuedKey = "ExecutorNumQueuedRequests";
  const preemptionTimeLostKey = "PreemptionTimeLost";

  function addRow(table, data, plot) {
    var row = $("<tr>");
    table.append(row);
    for (var i in data) {
      var item = $("<td>");
      item.append(data[i][data[i].length - 1]);
      row.append(item);
      if (plot[i]) {
        var line = $("<span>");
        line.css("padding-left", "0.5em");
        item.append(' ');
        item.append(line);
        line.sparkline(data[i]);
      }
    }
  }

  function createTable(table, data, plot) {
    table.empty();
    addRow(table, data, plot);
  }

  function addLineItem(list, newItem, max) {
    max = max || 30;
    list.push(newItem);
    if(list.length < max) {
      while(list.length < max) {
        list.push(0);
      }
    } else if (list.length > max) {
      while(list.length > max) {
        list.shift();
      }
    }
  }

  var heapUsed = [], heapMax = [], heapUseRate = [];
  var cacheUsed = [], cacheMax = [], cacheUseRate = [],
      cacheRequest = [], cacheHit = [];
  var executorsUsed = [], numExecutors = [], executorUseRate = [],
      queue = [], execAndQueue = [], preemptionTimeLost = [];

  $("#hostname").append(location.hostname);
  setInterval(function() {
    $.getJSON("/jmx", function(jmx){
      var data = jmx["beans"];
      for (var i in data) {
        if (heapUsedKey in data[i]) addLineItem(heapUsed, data[i][heapUsedKey].toFixed(1));
        if (heapMaxKey in data[i]) addLineItem(heapMax, data[i][heapMaxKey].toFixed(1));

        if (cacheUsedKey in data[i]) addLineItem(cacheUsed, data[i][cacheUsedKey] / 1024 / 1024); // Exchange Byte to MB
        if (cacheMaxKey in data[i]) addLineItem(cacheMax, data[i][cacheMaxKey] / 1024 / 1024);

        if (cacheHitKey in data[i]) addLineItem(cacheHit, data[i][cacheHitKey] * 100);
        if (cacheRequestKey in data[i]) addLineItem(cacheRequest, data[i][cacheRequestKey]);

        if (numExecutorsKey in data[i]) addLineItem(numExecutors, data[i][numExecutorsKey]);
        if (executorUsedKey in data[i]) addLineItem(executorsUsed, data[i][executorUsedKey].length);
        if (numQueuedKey in data[i]) addLineItem(queue, data[i][numQueuedKey]);
        if (preemptionTimeLostKey in data[i]) addLineItem(preemptionTimeLost, data[i][preemptionTimeLostKey]);
      }

      addLineItem(heapUseRate, (heapUsed[heapUsed.length - 1] / heapMax[heapMax.length - 1] * 100).toFixed(1));
      createTable($("#heap_body"),
                  [heapUsed, heapMax, heapUseRate],
                  [false, false, true]);
      addLineItem(cacheUseRate, (cacheUsed[cacheUsed.length - 1] / cacheMax[cacheMax.length - 1] * 100).toFixed(1));
      createTable($("#cache_body"),
                  [cacheUsed, cacheMax, cacheUseRate, cacheRequest, cacheHit],
                  [false, false, true, false, true]);
      addLineItem(executorUseRate,
                  executorsUsed[executorsUsed.length - 1] / numExecutors[numExecutors.length - 1] * 100);
      addLineItem(execAndQueue, executorsUsed[executorsUsed.length - 1] + queue[queue.length - 1]);
      createTable($("#executors_body"),
                  [executorsUsed, numExecutors, executorUseRate, queue, execAndQueue, preemptionTimeLost],
                  [false, false, true, false, true, false]);
    });
  }, 1000); // Update par sec
});
