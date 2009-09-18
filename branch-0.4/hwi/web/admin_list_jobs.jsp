<%@page errorPage="error_page.jsp" %>
<%@ page import="org.apache.hadoop.hive.hwi.*,org.apache.hadoop.hive.ql.exec.ExecDriver" %>
<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>
<html>
  <head>
    <title>Hive Web Interface</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  <jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Hive Web Interface</h2>
          <p>List Hive JobTracker Jobs</p>
          <table border="1">
          	<tr>
          		<td>Job ID</td>
          		<td>Kill URL</td>
          	</tr>
          <% for (String id: ExecDriver.runningJobKillURIs.keySet() ){ %>
            <tr>
          		<td><%=id%></td>
          		<td><%=ExecDriver.runningJobKillURIs.get(id)%></td>
          	</tr>
          <% } %>
        </td>
      </tr>
    </table>
  </body>
</html>
