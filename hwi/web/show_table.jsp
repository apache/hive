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
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">
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
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>HWI Hive Web Interface-Schema Browser</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top"><jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          
          <h2><%=table%></h2>
          ColsSize: <%= sd.getColsSize()%> <br>
          Input Format: <%= sd.getInputFormat()   %><br>
          Output Format: <%= sd.getOutputFormat()  %><br>
          Is Compressed?: <%= sd.isCompressed()  %><br>
          Location: <%= sd.getLocation()  %> <br>
          Number Of Buckets: <%= sd.getNumBuckets() %><br>
          
           <h2>Field Schema</h2>
          <table border="1">
            <tr>
              <td>Name</td>
              <td>Type</td>
              <td>Comment</td>
            </tr>
            <% for (FieldSchema fs: fsc ) {%>
            <tr>
              <td><%=fs.getName() %></td>
              <td><%=fs.getType() %></td> 
              <td><%=fs.getComment() %></td>
            </tr>
            <% } %>
          </table>
          
          <table border="1">
            <tr><td>Bucket Columns</td></tr>
            <% for (String col: bcols ) {%>
            <tr><td><%=col%></td></tr>
            <% } %>
          </table>
          
          <h2>Sort Columns</h2>
          <table border="1">
            <tr>
              <td>Column</td>
              <td>Order</td>
            </tr>
            <% for (Order  o: ord ) {%>
            <tr>
              <td><%= o.getCol()   %></td>
              <td><%= o.getOrder()   %></td>
            </tr>
            <% } %>
          </table>
          <h2>Parameters</h2>
          <table border="1">
            <tr>
              <td>Name</td>
              <td>Value</td>
            </tr>
          <%  for ( String key: sd.getParameters().keySet() ){  %>
            <tr>
              <td><%=key%></td>
              <td><%=sd.getParameters().get(key)%></td>
            </tr>
          <% } %>
          </table>
          
          <h2>SerDe Info</h2>
          <% SerDeInfo si = sd.getSerdeInfo(); %>
          Name:<%= si.getName() %><br>
          Lib: <%= si.getSerializationLib()  %><br>
            <table border="1">
              <tr>
                <td>Name</td>
                <td>value</td>
              </tr>
              <% for (String key: si.getParameters().keySet() ) { %>
              <tr>
                <td><%= key %></td>
                <td><%= si.getParameters().get(key)  %></td>
              </tr>
              <% } %>
            </table>
            
            <h2>Partition Information</h2>
             <table border="1">
              <tr>
         		<td>Name</td>
                <td>Type</td>
                <td>Comment</td>
              </tr>
            <% for (FieldSchema fieldSchema: t.getPartitionKeys() ){ %>
           	  <tr>
            	<td><%= fieldSchema.getName() %></td>
                <td><%= fieldSchema.getType() %></td>
                <td><%= fieldSchema.getComment() %></td>
              </tr>
   			<% } %>
        </td>
      </tr>
    </table>
  </body>
</html>
<% client.close(); %>