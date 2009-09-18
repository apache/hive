<%@ page import="org.apache.hadoop.hive.hwi.*" %>
<%@page errorPage="error_page.jsp" %>
<% 
  HWIAuth auth = (HWIAuth) session.getAttribute("auth"); 
  if ( auth == null ){
    auth = new HWIAuth();
    auth.setUser("");
    auth.setGroups( new String [] { "" });
    session.setAttribute("auth", auth);
  }
%>
<%
  String user = request.getParameter("user");
  String groups = request.getParameter("groups");
  if (user != null){
    auth.setUser(user);
    auth.setGroups( groups.split("\\s+") );
    session.setAttribute("auth", auth);
  }
%>
<html>
  <head>
    <title>Authorize</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" width="100">
	  <jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Change User Info</h2>

	<% if (request.getParameter("user")!=null){ %>
		<font color="red"><b>Authorization is complete.</b></font>
	<% }  %>
	  <form action="authorize.jsp">
	    <table border="1">
	      <tr>
	        <td>User</td>
		<td><input type="text" name="user" value="<%=auth.getUser()%>"></td>
	      </tr>
              <tr>
		<td>Groups</td>
		<td><input type="text" name="groups" value="<% 
			for (String group:auth.getGroups() ){
				out.print(group);
			}
		%>"></td>
	      </tr>
	      <tr>
	        <td colSpan="2"><input type="submit"></td>
              </tr>
	    </table>
	  </form>

        </td>
      </tr>
    </table>
  </body>
</html>