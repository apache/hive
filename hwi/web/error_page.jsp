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
<%@page isErrorPage="true"%>
<!DOCTYPE html>
<html>
<head>
<title>Hive Web Interface</title>
<link href="css/bootstrap.min.css" rel="stylesheet">
</head>
<body style="padding-top: 60px;">
    <jsp:include page="/navbar.jsp"></jsp:include>
	<div class="container">
		<div class="row">
			<div class="span4">
				<jsp:include page="/left_navigation.jsp" />
			</div>
			<!-- span4 -->
			<div class="span8">
				<h2>Hive Web Interface</h2>
				<div class="alert alert-error">
					<h4 class="alert-heading"><%= exception.getClass().getName() %></h4>
					<%=exception.getMessage() %>
				</div>
				<!-- alert -->
				<h3>Stacktrace</h3>
				<pre class="pre-scrollable">
          <% for (StackTraceElement e: exception.getStackTrace() ) { %>
          	File: <%= e.getFileName() %> Line:<%= e.getLineNumber() %> method: <%= e.getMethodName() %>
          	class: <%=e.getClassName() %>
          <% }  %>
          </pre>
			</div>
			<!-- span8 -->
		</div>
		<!-- row -->
	</div>
	<!-- container -->
</body>
</html>
