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
          a user can start HWIWebSessions. A HWIWebSession is roughly equal to the hive shell from the console window. 
          Users have access to the SetProcessor and QueryProcessor. After initiating a query the state is kept on the 
          web server. They can return later to view the status of the query and view any results it produced. </p>
        </td>
      </tr>
    </table>
  </body>
</html>
