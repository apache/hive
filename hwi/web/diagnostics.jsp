<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@page errorPage="error_page.jsp" %>
<%@page import="java.util.*" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">

<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Diagnostics</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" width="100"><jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          
          <h2>System.getProperties()</h2>
          <table border="1">
            <tr>
              <td>Name</td>
              <td>Value</td>
            </tr>
            <%
          Properties p = System.getProperties();
          for (Object o : p.keySet()) {%>
            <tr>
              <td><%=o%></td>
              <td><%=p.getProperty(((String) o))%></td>
            </tr>
            <% }%>
          </table>
          
               <h2>System.getenv()</h2>
          <table border="1">
            <tr>
              <td>Name</td>
              <td>Value</td>
            </tr>
            <%
           Map<String,String> env =  System.getenv();
          for (String key : env.keySet() ) {%>
            <tr>
              <td><%=key%></td>
              <td><%=env.get(key)%></td>
            </tr>
            <% }%>
          </table>
          
          
        </td>
      </tr>
    </table>
  </body>
</html>