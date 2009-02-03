<%@ page import="org.apache.hadoop.hive.hwi.*" %>
<%@ page errorPage="error_page.jsp" %>
<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>
<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>
<% String sessionName=request.getParameter("sessionName"); %>
<% String message=null; %>
<% 
  if (request.getParameter("confirm")!=null){ 
    HWISessionItem i = hs.findSessionItemByName(auth,sessionName);	  
	i.clientKill();
	message="Query will be killed";
  }
%>
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
          <% if (message!=null){ %><font color="red"><%=message%></font><% } %>
          <br>
          <form action="session_kill.jsp">
          	<input type="hidden" name="sessionName" value="<%=sessionName%>">
          	Are you sure you want to kill this session?
          	<input type="submit" name="confirm" value="yes">
          </form>
        </td>
      </tr>
    </table>
  </body>
</html>
