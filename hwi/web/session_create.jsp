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
<%@ page errorPage="error_page.jsp" %>
<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>

<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>

<% String sessionName=request.getParameter("sessionName"); %>
<% String message = null; %>

<% 
	if (sessionName != null){
		if (sessionName.equals("")){
			message="This is not a valid session name";
		} else {
			HWISessionItem item= hs.findSessionItemByName(auth, sessionName);
			if (item!=null){
				message="This name is already in use";
			} else {
				hs.createSession(auth,sessionName);
				RequestDispatcher rd = application.getRequestDispatcher("/session_manage.jsp");
				rd.forward(request,response);
			}
		}
	}

	if (sessionName == null){
		sessionName="";
	}
%>

<html>
  <head>
    <title>Hive Web Interface-Create a Hive Session</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  <jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Create a Hive Session</h2>
          <form action="session_create.jsp">
            <table border="1">
            <tr>
              <td>Session Name</td>
              <td><input type="text" name="sessionName" value="<%=sessionName%>" ></td>
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