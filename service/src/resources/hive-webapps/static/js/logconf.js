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

    // init the table with the current loggers
    loadLoggers();

    // set up event handler for submitting the form
    $("#log-level-submit").click(function(e) {
        setLoggersWithLevel(e);
    });
});

function setLoggersWithLevel(e) {
    console.log("handler called");
    var loggerName = sanitize($("#logger-name").val());
    var logLevel = $("#log-level").val();
    var data = JSON.stringify( { "loggers" : [ { "logger" : loggerName, "level" : logLevel } ] } );

    $.post('conflog', data, function() {
        loadLoggers();
    });
}

function loadLoggers() {
    // clear the current content
    $("#current-logs").html("");

    // load and render the new
    $.getJSON('conflog', function (data) {
        var loggers = data.loggers;

        $.each(loggers, function(i, logger) {
            var logger_information = "<tr>\n" +
                "                        <td>" + logger.logger + "</td>\n" +
                "                        <td>" + logger.level + "</td>\n" +
                "                    </tr>";
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
