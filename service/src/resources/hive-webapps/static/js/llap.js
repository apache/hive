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

$(document).ready(function () {
    var showData = $('#show-data');

    $.getJSON('llap', function (data) {
        console.log(data);

        showData.empty();
        var node = JsonHuman.format(data, window.options);
        showData[0].appendChild(node);
    });
    showData.text('Loading the JSON file.');
});
