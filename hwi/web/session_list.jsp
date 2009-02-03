<%@ page import="org.apache.hadoop.hive.hwi.*" %>
<%@page errorPage="error_page.jsp" %>
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

<html>
  <head>
    <title>Session List</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  <jsp:include page="left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Session List</h2>
         
			<table border="1">
			  <tr>
			    <td>Name</td>
			    <td>Status</td>
			    <td>Action</td>
			  </tr>
			   <% if ( hs.findAllSessionsForUser(auth)!=null){ %>
				  <% for (HWISessionItem item: hs.findAllSessionsForUser(auth) ){ %>
				  	<tr>
				  	  <td><%=item.getSessionName()%></td>
				  	  <td><%=item.getStatus()%></td>
				  	  <td><a href="/hwi/session_manage.jsp?sessionName=<%=item.getSessionName()%>">Manager</a></td>
				  	</tr>
				  <% } %>
			  <% } %>
			</table>          
        </td>
      </tr>
    </table>
  </body>
</html>

