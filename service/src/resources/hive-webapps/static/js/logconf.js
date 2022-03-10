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
