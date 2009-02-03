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
    if (i.getStatus() == HWISessionItem.WebSessionItemStatus.QUERY_COMPLETE || 
    		i.getStatus() == HWISessionItem.WebSessionItemStatus.NEW){
		hs.findAllSessionsForUser(auth).remove(i);
		message="Session removed";
    } else {
    	message="Session could not be removed";
    }
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
          <form action="session_remove.jsp">
          	<input type="hidden" name="sessionName" value="<%=sessionName%>">
          	Are you sure you want to remove this session?
          	<input type="submit" name="confirm" value="yes">
          </form>
        </td>
      </tr>
    </table>
  </body>
</html>
