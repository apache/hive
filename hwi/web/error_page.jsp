<%@ page import="org.apache.hadoop.hive.hwi.*" %>
<%@page isErrorPage="true" %>
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
          <p>
          <%= exception.getClass().getName() %>
          <br>
          <%=exception.getMessage() %>
          <br>
          <% for (StackTraceElement e: exception.getStackTrace() ) { %>
          	File: <%= e.getFileName() %> Line:<%= e.getLineNumber() %> method: <%= e.getMethodName() %>
          	class: <%=e.getClassName() %> <br>
          <% }  %>
          </p>
        </td>
      </tr>
    </table>
  </body>
</html>
