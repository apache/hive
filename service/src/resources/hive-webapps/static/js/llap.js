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
window.options = {
    showArrayAsFlatTable: true,
    flatTableKeys: ['containerId','webUrl'],

    showArrayIndex: false,
    hyperlinks : {
        enable : true,
        keys: ['amWebUrl','statusUrl','webUrl'],
        target : '_blank'
    },

    bool : {
        showText : true,
        text : {
            true : "true",
            false : "false"
        },
        showImage : true,
        img : {
            true : 'css/true.png',
            false : 'css/false.png'
        }
    }
};

function llapState(title, msg, kind) {
    var tone = kind === 'error'
        ? 'text-red-300 dark:text-red-700'
        : 'text-slate-300 dark:text-slate-600';
    var icon = "<svg class='w-10 h-10 mx-auto mb-3 " + tone + "' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'>" +
        "<rect x='3' y='4' width='18' height='6' rx='1'/><rect x='3' y='14' width='18' height='6' rx='1'/><path d='M7 7h.01M7 17h.01'/></svg>";
    return "<div class='py-16 text-center'>" + icon +
        "<div class='text-base font-medium text-slate-700 dark:text-slate-200'>" + title + "</div>" +
        "<div class='mt-1.5 text-sm text-slate-400 max-w-md mx-auto'>" + msg + "</div></div>";
}

$(document).ready(function () {
    var showData = $('#show-data');
    showData.html(llapState("Loading LLAP status", "Querying daemons\u2026", "info"));

    $.getJSON('llap', function (data) {
        showData.empty();
        var str = "";
        try { str = JSON.stringify(data) || ""; } catch (e) {}
        var noDaemons = !data || Object.keys(data).length === 0 || str.indexOf("No llap daemons") >= 0;
        if (noDaemons) {
            showData.html(llapState(
                "No LLAP daemons",
                "LLAP is not enabled on this HiveServer2. Configure " +
                "<code class='font-mono text-xs px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300'>hive.llap.daemon.service.hosts</code> to run daemons.",
                "info"));
            return;
        }
        var node = JsonHuman.format(data, window.options);
        showData[0].appendChild(node);
    }).fail(function () {
        showData.html(llapState("Could not load LLAP status", "The /llap endpoint did not respond.", "error"));
    });
});
