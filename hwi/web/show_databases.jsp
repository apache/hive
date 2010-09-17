<%@page import="org.apache.hadoop.hive.metastore.*,
org.apache.hadoop.hive.metastore.api.*,
org.apache.hadoop.hive.conf.HiveConf,
org.apache.hadoop.hive.ql.session.SessionState,
java.util.*,
org.apache.hadoop.hive.ql.*,
org.apache.hadoop.hive.cli.*" %>
<%@page errorPage="error_page.jsp" %>
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
"http://www.w3.org/TR/html4/loose.dtd">
<% 
  HiveConf hiveConf = new HiveConf(SessionState.class); 
  HiveMetaStoreClient client = new HiveMetaStoreClient(hiveConf);
  List <String> dbs = client.getAllDatabases();
  client.close();
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
          
          <h2>Database List</h2>
          <table border="1">
          <% for (String db : dbs) { %>
          <tr><td><a href="/hwi/show_database.jsp?db=<%=db%>"><%=db%></a>
          </td></tr>
          <% } %>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>
