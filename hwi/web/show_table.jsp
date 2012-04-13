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
<%@page errorPage="error_page.jsp" %>
<%@page import="java.util.*" %>
<jsp:include page="/include/session_credentials.jsp" />
<%@page import="org.apache.hadoop.hive.metastore.*,
org.apache.hadoop.hive.metastore.api.*,
org.apache.hadoop.hive.conf.HiveConf,
org.apache.hadoop.hive.ql.session.SessionState,
java.util.*,
org.apache.hadoop.hive.ql.*,
org.apache.hadoop.hive.cli.*" %>
<% 
  String db= request.getParameter("db");
  String table= request.getParameter("table");
  
  HiveConf hiveConf = new HiveConf(SessionState.class); 
  HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
  org.apache.hadoop.hive.metastore.api.Table t = client.getTable(db, table);
  StorageDescriptor sd = t.getSd();
  List<String> bcols = sd.getBucketCols();
  List<FieldSchema> fsc = sd.getCols();
  List<Order> ord = sd.getSortCols();
  Partition p = null;//How do we get this info?
%>
<!DOCTYPE html>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>HWI Hive Web Interface-Schema Browser</title>
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
				<h2><%=table%></h2>
				ColsSize:
				<%= sd.getColsSize()%>
				<br> Input Format:
				<%= sd.getInputFormat()   %><br> Output Format:
				<%= sd.getOutputFormat()  %><br> Is Compressed?:
				<%= sd.isCompressed()  %><br> Location:
				<%= sd.getLocation()  %>
				<br> Number Of Buckets:
				<%= sd.getNumBuckets() %><br>

				<h2>Field Schema</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Type</th>
							<th>Comment</th>
						</tr>
					</thead>
					<tbody>
						<% for (FieldSchema fs: fsc ) {%>
						<tr>
							<td><%=fs.getName() %></td>
							<td><%=fs.getType() %></td>
							<td><%=fs.getComment() %></td>
						</tr>
						<% } %>
					</tbody>
				</table>

				<table class="table table-striped">
					<thead>
						<tr>
							<th>Bucket Columns</th>
						</tr>
					</thead>
					<tbody>
						<% for (String col: bcols ) {%>
						<tr>
							<td><%=col%></td>
						</tr>
						<% } %>
					</tbody>
				</table>

				<h2>Sort Columns</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Column</th>
							<th>Order</th>
						</tr>
					</thead>
					<tbody>
						<% for (Order  o: ord ) {%>
						<tr>
							<td><%= o.getCol()   %></td>
							<td><%= o.getOrder()   %></td>
						</tr>
						<% } %>
					</tbody>
				</table>

				<h2>Parameters</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Value</th>
						</tr>
					</thead>
					<tbody>
						<%  for ( String key: sd.getParameters().keySet() ){  %>
						<tr>
							<td><%=key%></td>
							<td><%=sd.getParameters().get(key)%></td>
						</tr>
						<% } %>
					</tbody>
				</table>

				<h2>SerDe Info</h2>
				<% SerDeInfo si = sd.getSerdeInfo(); %>
				Name:<%= si.getName() %><br> Lib:
				<%= si.getSerializationLib()  %><br>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>value</th>
						</tr>
					</thead>
					<tbody>
						<% for (String key: si.getParameters().keySet() ) { %>
						<tr>
							<td><%= key %></td>
							<td><%= si.getParameters().get(key)  %></td>
						</tr>
						<% } %>
					</tbody>
				</table>

				<h2>Partition Information</h2>
				<table class="table table-striped">
					<thead>
						<tr>
							<th>Name</th>
							<th>Type</th>
							<th>Comment</th>
						</tr>
					</thead>
					<tbody>
						<% for (FieldSchema fieldSchema: t.getPartitionKeys() ){ %>
						<tr>
							<td><%= fieldSchema.getName() %></td>
							<td><%= fieldSchema.getType() %></td>
							<td><%= fieldSchema.getComment() %></td>
						</tr>
						<% } %>
					</tbody>
				</table>

			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
<% client.close(); %>
