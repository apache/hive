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
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@page errorPage="error_page.jsp"%>
<%@page import="java.util.*"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Diagnostics</title>
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

				<h2>System.getProperties()</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Value</th>
						</tr>
					</thead>
					<tbody>
						<%
          Properties p = System.getProperties();
          for (Object o : p.keySet()) {%>
						<tr>
							<td><%=o%></td>
							<td><%=p.getProperty(((String) o))%></td>
						</tr>
						<% }%>
					</tbody>
				</table>

				<h2>System.getenv()</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Value</th>
						</tr>
					</thead>
					<tbody>
						<%
           Map<String,String> env =  System.getenv();
          for (String key : env.keySet() ) {%>
						<tr>
							<td><%=key%></td>
							<td><%=env.get(key)%></td>
						</tr>
						<% }%>
					</tbody>
				</table>


			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
