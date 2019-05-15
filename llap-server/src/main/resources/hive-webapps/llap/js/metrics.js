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

/* llap (singletons) */
var llap = {"model" : {}, "view" : {}}

if (!("console" in window)) {
   window.console = {"log" : function() {/*nothing*/}};
}

(
/* anonymous closure to prevent variable leakage */
function () {

var trendlist = function(max) {
   var list = [];
   list.fill(0,0,max);
   list.peek = function() {
      if(this.length != 0) { return this[this.length-1] }
   }
   list.add = function(v) {
      while (this.length < max) {
         this.push(0);
      }
      this.push(v);
      while(this.length > max) {
         this.shift();
      }
   }
   return list
}

var jmxbean = function(jmx, name) {
   var beans = jmx["beans"].filter(function(b, i, a) { return b.name.match(name) });
   return beans.pop();
}


llap.model.JvmMetrics = new function() {
   this.name = "Hadoop:service=LlapDaemon,name=JvmMetrics";
   this.heap_used = 0;
   this.heap_max = 0;
   this.heap_rate = trendlist(50);
   this.gc_times = trendlist(50);
   this.old_gc = 0;
   this.push = function(jmx) {
      var bean = jmxbean(jmx, this.name);
      this.hostname = bean["tag.Hostname"];
      this.heap_max = bean["MemHeapMaxM"];
      this.heap_used = bean["MemHeapUsedM"];
      this.heap_rate.add((this.heap_used*100.0)/this.heap_max);
      var new_gc = (bean["GcTimeMillis"]/ 1000.0); 
      if (this.old_gc != 0) {
         this.gc_times.add((new_gc - this.old_gc) / 1000.0);
      }
      this.old_gc = new_gc; 
   }
   return this;
}

llap.model.LlapDaemonCacheMetrics = new function() {
   this.name = "Hadoop:service=LlapDaemon,name=LlapDaemonCacheMetrics";
   this.hit_rate = trendlist(50);
   this.fill_rate = trendlist(50);
   this.push = function(jmx) {
      var bean = jmxbean(jmx, this.name);
      if (bean) {
        this.cache_max = bean["CacheCapacityTotal"]/(1024*1024);
        this.cache_used = bean["CacheCapacityUsed"]/(1024*1024);
        this.cache_reqs = bean["CacheReadRequests"];
        this.fill_rate.add((this.cache_used*100.0)/this.cache_max);
        this.hit_rate.add(bean["CacheHitRatio"]*100.0);
      } else {
        this.cache_max = -1;
        this.cache_used = -1;
        this.cache_reqs = -1;
        this.fill_rate.add(0);
        this.hit_rate.add(-1);
      }
   }
   return this;
}


llap.model.LlapDaemonInfo = new function() {
   this.name = "Hadoop:service=LlapDaemon,name=LlapDaemonInfo";
   this.active_rate = trendlist(50);
   this.push = function(jmx) {
      var bean = jmxbean(jmx, this.name); 
      this.executors = bean["NumExecutors"];
      this.active = bean["NumActive"];
      this.active_rate.add(this.active);
   }
}

llap.model.LlapDaemonExecutorMetrics = new function() {
   this.name = "Hadoop:service=LlapDaemon,name=LlapDaemonExecutorMetrics";
   this.queue_rate = trendlist(50);
   this.push = function(jmx) {
      var bean = jmxbean(jmx, this.name);
      this.queue_rate.add(bean["ExecutorNumQueuedRequests"] || 0);
      this.lost_time = bean["PreemptionTimeLost"] || 0;
      this.num_tasks = bean["ExecutorTotalRequestsHandled"];
      this.interrupted_tasks = bean["ExecutorTotalInterrupted"] || 0;
      this.failed_tasks = bean["ExecutorTotalExecutionFailure"] || 0;
   }
   return this;
}


llap.model.OperatingSystem = new function() {
	this.name = "java.lang:type=OperatingSystem";
	this.sys_cpu_rate = trendlist(50);
	this.proc_cpu_rate = trendlist(50);
	this.loadavg_rate = trendlist(50);
	this.used_ram_rate = trendlist(50);
	this.open_fds_rate = trendlist(50);
	this.push = function(jmx) {
		var bean = jmxbean(jmx, this.name);
		this.sys_cpu_rate.add(Math.max(bean["SystemCpuLoad"]-bean["ProcessCpuLoad"],0)*100);
		this.proc_cpu_rate.add(bean["ProcessCpuLoad"]*100);
		this.loadavg_rate.add(bean["SystemLoadAverage"]);
		this.used_ram_rate.add(100*(bean["TotalPhysicalMemorySize"] - bean["FreePhysicalMemorySize"])/bean["TotalPhysicalMemorySize"]);
		this.open_fds_rate.add(bean["OpenFileDescriptorCount"]);
		this.cpu_cores = bean["AvailableProcessors"];
	}
}

llap.view.Hostname = new function () {
   this.refresh = function() {
      $("#hostname").text(llap.model.JvmMetrics.hostname);
   }
}

llap.view.Heap = new function () {
   this.refresh = function() {
      var model = llap.model.JvmMetrics; 
      $("#heap-used").text(model.heap_used.toFixed(2));
      $("#heap-max").text(model.heap_max.toFixed(2));
      $("#heap-rate").text(model.heap_rate.peek().toFixed(2));
      $("#heap-trend").sparkline(model.heap_rate);
      $("#heap-gc").text(model.old_gc.toFixed(2));
      $("#heap-gc-trend").sparkline(model.gc_times, {type: 'bar', barColor: 'red'});
   }
}

llap.view.Cache = new function () {
   this.refresh = function() {
      var model = llap.model.LlapDaemonCacheMetrics;
      $("#cache-used").text(model.cache_used.toFixed(2));
      $("#cache-max").text(model.cache_max.toFixed(2));
      $("#cache-fill-rate").text(model.fill_rate.peek().toFixed(2));
      $("#cache-fill-trend").sparkline(model.fill_rate);
      $("#cache-requests").text(model.cache_reqs.toFixed(0));
      $("#cache-hits").text(model.hit_rate.peek().toFixed(2));
      $("#cache-hits-trend").sparkline(model.hit_rate);
   }
}

llap.view.Executors = new function () {
   this.refresh = function() {
      var model = llap.model.LlapDaemonInfo;
      $("#executors-used").text(model.active);
      $("#executors-max").text(model.executors);
      $("#executors-rate").text(((model.active_rate.peek() * 100.0)/model.executors).toFixed(0));
      $("#executors-trend").sparkline(model.active_rate);

      var model1 = llap.model.LlapDaemonExecutorMetrics;
      $("#executors-queue").text(model1.queue_rate.peek().toFixed(0));
      $("#executors-pending").text((model1.queue_rate.peek() + model.active_rate.peek()).toFixed(0));
      $("#executors-pending-trend").sparkline(model1.queue_rate.map(
         function(v,i) { return v + model.active_rate[i] }
      ));
   }
}

llap.view.Tasks = new function() {
   this.refresh = function() {
      var model = llap.model.LlapDaemonExecutorMetrics;
	  $("#fragments-total").text(model.num_tasks);
	  $("#fragments-failed").text(model.failed_tasks);
	  $("#fragments-preempted").text(model.interrupted_tasks);
      $("#fragments-preemption-time").text((model.lost_time / 1000.0).toFixed(3));
   }
}

llap.view.System = new function() {
	this.refresh = function() {
		var model = llap.model.OperatingSystem;
		$("#proc-cores").text(model.cpu_cores);
		$("#proc-cpu").text(model.proc_cpu_rate.peek().toFixed(2));
		$("#proc-cpu-trend").sparkline(model.proc_cpu_rate.map(
		  function(v, i) { return [model.sys_cpu_rate[i],v]; }
		), {type: 'bar', stackedBarColor: ['red','green']});

		$("#system-loadavg").text(model.loadavg_rate.peek());
		$("#system-loadavg-trend").sparkline(model.loadavg_rate);

		$("#system-ram").text(model.used_ram_rate.peek().toFixed(2));
		$("#system-ram-trend").sparkline(model.used_ram_rate);

		$("#proc-fds").text(model.open_fds_rate.peek().toFixed(0));
		$("#proc-fds-trend").sparkline(model.open_fds_rate);
	}
}

})();

$(function() {
  var models = [llap.model.JvmMetrics, llap.model.LlapDaemonCacheMetrics, llap.model.LlapDaemonExecutorMetrics, llap.model.LlapDaemonInfo, llap.model.OperatingSystem]

  var views = [llap.view.Hostname, llap.view.Heap, llap.view.Cache, llap.view.Executors, llap.view.Tasks, llap.view.System]

  setInterval(function() {
    $.getJSON("jmx", function(jmx){
      models.forEach(function (m) { m.push(jmx); });
      views.forEach(function (v) { v.refresh(); });
    });
  }, 1000); // Update par sec
});
