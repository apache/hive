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
<%@page contentType="text/html" pageEncoding="UTF-8" %>
<%@page errorPage="error_page.jsp" %>
<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page import="org.apache.hadoop.hive.conf.*" %>
<%@page import="java.util.*" %>

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
<% String sessionName = request.getParameter("sessionName"); %>
<% HWISessionItem si = hs.findSessionItemByName(auth,sessionName); %>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Session Result Bucket</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top"><jsp:include page="left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Session Result Bucket</h2>
          
          <% if (si!=null) { %>
            <table border=1">
              <% for (List<String> rows : si.getResultBucket() ) { %>
	        <% for (String row : rows ) { %>  
		  <tr><td><%=row%></td></tr>
	      	<% } %>
            <% } %>
	    	</table>	
          <% } %>
          
        </td>
      </tr>
    </table>
  </body>
</html>
