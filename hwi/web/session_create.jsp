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
<%@ page import="org.apache.hadoop.hive.hwi.*"%>
<%@ page errorPage="error_page.jsp"%>
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
<!DOCTYPE html>
<html>
<head>
<title>Hive Web Interface-Create a Hive Session</title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span4">
				<jsp:include page="/left_navigation.jsp" />
			</div><!-- span4 -->
			<div class="span8">
				<form action="session_create.jsp" class="form-horizontal">
					<fieldset>
						<legend>Create a Hive Session</legend>
						<div class="control-group">
							<label class="control-label" for="fldsession">Sesion Name</label>
							<div class="controls">
								<input id="fldsession" type="text" name="sessionName"
									value="<%=sessionName%>">
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
