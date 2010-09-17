<%@page errorPage="error_page.jsp" %>
<%@page import="org.apache.hadoop.hive.metastore.*,
org.apache.hadoop.hive.metastore.api.*,
org.apache.hadoop.hive.conf.HiveConf,
org.apache.hadoop.hive.ql.session.SessionState,
java.util.*,
org.apache.hadoop.hive.ql.*,
org.apache.hadoop.hive.cli.*" %>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
   "http://www.w3.org/TR/html4/loose.dtd">
<%
  HiveConf hiveConf = new HiveConf(SessionState.class); 
  HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
  String db = request.getParameter("db");
  Database db2 = client.getDatabase(db);
  List<String> tables = client.getAllTables(db);
  client.close();
%>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>HWI Hive Web Interface</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top"><jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2><%= db%> Table List </h2>
          Name: <%=  db2.getName()%><br>
          Description: <%=  db2.getDescription()%><br> 
          
          <table border="1">
          <% for (String table : tables){ %>
          <tr><td><a href="/hwi/show_table.jsp?db=<%=db%>&table=<%=table%>"><%=table%></a></td></tr>
          <% } %>
          </table>
          
        </td>
      </tr>
    </table>
  </body>
</html>
