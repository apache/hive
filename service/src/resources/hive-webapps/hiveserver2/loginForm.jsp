<%@ page contentType="text/html;charset=UTF-8" %>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <meta name="description" content="Login - Hive">
        <title>HiveServer2 - WebUI</title>
        <link rel="icon" href="/static/favicon.ico">
        <link href="/static/css/bootstrap.min.css" rel="stylesheet">
        <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
        <link href="/static/css/hive.css" rel="stylesheet">
        <link href="/static/css/login.css" rel="stylesheet">
    </head>
    <body>
        <div class="login-form">
            <form id="login" action="login" method="post">
                <h2 class="text-center">Log in</h2>
                <div class="form-group">
                    <input id="username" name="username" type="text" class="form-control" placeholder="Username" required="required">
                </div>
                <div class="form-group">
                    <input id="password" name="password" type="password" class="form-control" placeholder="Password" required="required">
                </div>
                <input id="redirectPath" name="redirectPath" type="hidden">
                <div class="form-group">
                    <button id="submit" type="submit" class="btn btn-primary btn-block">Log In</button>
                </div>
            </form>
        </div>
    </body>
</html>