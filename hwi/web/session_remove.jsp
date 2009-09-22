<%--
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
--%>
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
    if (i.getStatus() != HWISessionItem.WebSessionItemStatus.QUERY_RUNNING ){ 
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
