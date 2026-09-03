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
// Current logger name -> level, populated from the /conflog response and used to
// pre-select the level of the logger chosen in the dropdown.
var currentLoggers = {};

$(document).ready(function () {

    // init the table and the logger dropdown with the current loggers
    loadLoggers();

    // keep the level dropdown in sync with the selected logger
    $("#logger-name").change(function () {
        syncLevelToSelectedLogger();
    });

    // set up event handler for submitting the form
    $("#log-level-submit").click(function (e) {
        setLoggerLevel(e);
    });
});

// Log4j2 uses the empty string as the name of the root logger; show it explicitly.
function displayName(loggerName) {
    return loggerName === "" ? "(root)" : loggerName;
}

function loadLoggers() {
    $.getJSON('conflog', function (data) {
        var loggers = (data.loggers || []).slice().sort(function (a, b) {
            return a.logger.localeCompare(b.logger);
        });

        currentLoggers = {};
        var logsTable = $("#current-logs").empty();
        var loggerSelect = $("#logger-name").empty();

        $.each(loggers, function (i, logger) {
            currentLoggers[logger.logger] = logger.level;

            // Build the row with text() so logger names/levels can never inject markup.
            var row = $("<tr>");
            $("<td>").text(displayName(logger.logger)).appendTo(row);
            $("<td>").text(logger.level).appendTo(row);
            logsTable.append(row);

            $("<option>").val(logger.logger).text(displayName(logger.logger)).appendTo(loggerSelect);
        });

        syncLevelToSelectedLogger();
    });
}

function syncLevelToSelectedLogger() {
    var loggerName = $("#logger-name").val();
    if (loggerName !== null && currentLoggers.hasOwnProperty(loggerName)) {
        $("#log-level").val(currentLoggers[loggerName]);
    }
}

function setLoggerLevel(e) {
    var loggerName = $("#logger-name").val();
    var logLevel = $("#log-level").val();
    if (loggerName === null) {
        return;
    }
    $("#logconf-error").hide();
    var data = JSON.stringify({ "loggers": [ { "logger": loggerName, "level": logLevel } ] });

    $.ajax({
        url: 'conflog',
        type: 'POST',
        contentType: 'application/json',
        // The endpoint replies 200 with an empty body; avoid jQuery trying to parse it as JSON.
        dataType: 'text',
        data: data
    }).done(function () {
        loadLoggers();
    }).fail(function (jqXHR) {
        showError(jqXHR);
    });
}

function showError(jqXHR) {
    var message = jqXHR.status === 401
        ? "You are not authorized to configure logging."
        : "Failed to update logger level (HTTP " + jqXHR.status + ").";
    $("#logconf-error").text(message).show();
}
