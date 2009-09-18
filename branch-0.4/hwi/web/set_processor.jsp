<%@page contentType="text/html" pageEncoding="UTF-8" %>
<%@page errorPage="error_page.jsp" %>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.conf.*" %>
<%@page import="java.util.*" %>
<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>

<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>


<% String sessionName = request.getParameter("sessionName"); %>
<% HWISessionItem sess = hs.findSessionItemByName(auth,sessionName); %>

<%
String setquery= request.getParameter("setquery");
String setaction= request.getParameter("setaction");
String message = null;
if (setaction != null){
  int ret = sess.runSetProcessorQuery(setquery);
  message =" Action taken "+ret;
}
%>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>HWI Set Processor</title>
  </head>
  <body>
    <% if (message !=null){ %>
      <font color="red"><b><%=message%></b></font>
    <% } %>
    <table>
      <tr>
        <td valign="top" width="100"><jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <form action="set_processor.jsp">
          <table border="1">
            <tr>
              <td>Set Query</td>
              <td><input type="text" name="setquery"></td>
            </tr>
            <tr>
            <td colspan="2">
              <input type="hidden" name="sessionName" value="<%=sessionName%>">
              <input type="submit" name="setaction" value="setaction"></td>
            </tr>
          </table>
          </form>
            
        </td>
      </tr>
    </table>
  </body>
</html>