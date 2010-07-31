<%@page errorPage="error_page.jsp" %>
<%@ page import="org.apache.hadoop.hive.hwi.*" %>
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
          <p>The Hive Web Interface (HWI) offers an alternative to the command line interface (CLI). Once authenticated 
          users can start HWIWebSessions. A HWIWebSession lives on the server users can submit queries and return later 
          to view the status of the query and view any results it produced. </p>
        </td>
      </tr>
    </table>
  </body>
</html>
