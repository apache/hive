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
<!DOCTYPE html>
<%@ page import="org.apache.hadoop.hive.hwi.*"%>
<%@page errorPage="error_page.jsp"%>
<%
	HWIAuth auth = (HWIAuth) session.getAttribute("auth");
	if (auth == null) {
		auth = new HWIAuth();
		auth.setUser("");
		auth.setGroups(new String[] { "" });
		session.setAttribute("auth", auth);
	}
%>
<%
	String user = request.getParameter("user");
	String groups = request.getParameter("groups");
	if (user != null) {
		auth.setUser(user);
		auth.setGroups(groups.split("\\s+"));
		session.setAttribute("auth", auth);
	}
%>
<html>
<head>
<title>Authorize</title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span4">
				<jsp:include page="/left_navigation.jsp" />
			</div>
			<div class="span8">

				<%
					if (request.getParameter("user") != null) {
				%>
				<div class="alert alert-success">
					<p>Authorization is complete.</p>
				</div>
				<%
					}
				%>
				<form action="authorize.jsp" class="form-horizontal">
					<fieldset>
					    <legend>Change User Info</legend>
						<div class="control-group">
							<label class="control-label" for="flduser">User</label>
							<div class="controls">
								<input id="flduser" type="text" name="user"
									value="<%=auth.getUser()%>">
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="fldgroups">Groups</label>
							<div class="controls">
								<input id="fldgroups" type="text" name="groups"
									value="<% for (String group : auth.getGroups()) { out.print(group); } %>">
							</div>
						</div>
					</fieldset>
					<div class="form-actions">
						<button type="submit" class="btn btn-primary">Submit</button>
					</div>
				</form>
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
