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
