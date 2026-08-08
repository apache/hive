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
$(document).ready(function () {

    // never serve /conflog from the browser cache, so the table reflects the
    // change immediately after Apply (no F5 needed)
    $.ajaxSetup({ cache: false });

    // init the table with the current loggers
    loadLoggers();

    // set up event handler for submitting the form
    $("#log-level-submit").click(function(e) {
        setLoggersWithLevel(e);
    });

    // when a logger is picked from the dropdown, reflect its current level
    $("#logger-name").on("change", syncLevel);
});

// set the level dropdown to the currently-picked logger's level
function syncLevel() {
    var lv = (window._loggerLevels || {})[$("#logger-name").val()];
    if (lv) { $("#log-level").val(String(lv).toUpperCase()); }
}

function setLoggersWithLevel(e) {
    var loggerName = $("#logger-name").val();
    var logLevel = $("#log-level").val();
    var data = JSON.stringify( { "loggers" : [ { "logger" : loggerName, "level" : logLevel } ] } );

    $.ajax({
        url: 'conflog', type: 'POST', data: data,
        // the endpoint replies 200 with an EMPTY body but a JSON content-type;
        // force text parsing so jQuery doesn't treat the empty body as a parse
        // error (which would wrongly fire the error handler on a success)
        dataType: 'text',
        success: function () {
            loadLoggers();
            if (window.hiveToast) hiveToast("Set " + (loggerName || "(root)") + " \u2192 " + logLevel, "ok");
        },
        error: function (xhr) {
            if (window.hiveToast) hiveToast("Failed to set log level" + (xhr && xhr.status ? " (HTTP " + xhr.status + ")" : "") + " \u2014 admin privileges required?", "err");
        }
    });
}

function loadLoggers() {
    // clear the current content
    $("#current-logs").html("");

    // load and render the new
    $.getJSON('conflog', function (data) {
        var loggers = data.loggers || [];

        // populate the logger-name dropdown (sorted, de-duplicated) + a level lookup
        window._loggerLevels = {};
        var seen = {}, picker = "";
        loggers.slice().sort(function (a, b) { return (a.logger || "").localeCompare(b.logger || ""); })
            .forEach(function (logger) {
                var key = logger.logger || "";
                window._loggerLevels[key] = logger.level;
                if (seen[key]) return; seen[key] = 1;
                picker += "<option value='" + key + "'>" + (key || "(root)") + "</option>";
            });
        var sel = $("#logger-name"), prev = sel.val();
        sel.html(picker);
        if (prev && window._loggerLevels.hasOwnProperty(prev)) { sel.val(prev); }
        syncLevel();

        $.each(loggers, function(i, logger) {
            var lvl = (logger.level || "").toUpperCase();
            var pc = "bg-slate-100 text-slate-600 dark:bg-slate-700/40 dark:text-slate-300";
            if (lvl === "ERROR" || lvl === "FATAL") pc = "bg-red-100 text-red-700 dark:bg-red-500/15 dark:text-red-400";
            else if (lvl === "WARN") pc = "bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-400";
            else if (lvl === "INFO") pc = "bg-emerald-100 text-emerald-700 dark:bg-emerald-500/15 dark:text-emerald-400";
            else if (lvl === "DEBUG" || lvl === "TRACE") pc = "bg-blue-100 text-blue-700 dark:bg-blue-500/15 dark:text-blue-300";
            var lname = logger.logger ? logger.logger : "(root)";
            var logger_information = "<tr class='hover:bg-slate-50 dark:hover:bg-slate-800/40'>" +
                "<td class='px-6 py-3 font-mono text-xs break-all'>" + lname + "</td>" +
                "<td class='px-6 py-3'><span class='inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-semibold " + pc + "'>" + logger.level + "</span></td>" +
                "</tr>";
            $("#current-logs").append(logger_information);
        });

    });
}

function sanitize(string) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#x27;',
        "/": '&#x2F;',
    };
    const reg = /[&<>"'/]/ig;
    return string.replace(reg, (match)=>(map[match]));
}
