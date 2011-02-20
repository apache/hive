<%--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
--%>
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