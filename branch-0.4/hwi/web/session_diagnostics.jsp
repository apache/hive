<%@page contentType="text/html" pageEncoding="UTF-8" %>
<%@page errorPage="error_page.jsp" %>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.conf.*" %>
<%@page import="java.util.*" %>

<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>
<% if (hs == null) { %>
  <jsp:forward page="error.jsp">
    <jsp:param name="message" value="Hive Session Manager Not Found" />
  </jsp:forward>
<% } %>

<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>
<% String sessionName = request.getParameter("sessionName"); %>
<% HWISessionItem si = hs.findSessionItemByName(auth,sessionName); %>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Session Diagnostics</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top"><jsp:include page="left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Session Diagnostics</h2>
          
          <% if (si!=null) { %>
            <table border="1">
              <tr>
                <td>Var Name</td>
                <td>Var Value</td>
              </tr>
            <% for (HiveConf.ConfVars var : HiveConf.ConfVars.values() ){ %>
              <tr>
                <td><%=var.name()%></td>
                <td><%=si.getHiveConfVar(var)%></td>
              </tr>
            <%  } %>
            </table>
          <% } %>
          
        </td>
      </tr>
    </table>
  </body>
</html>
