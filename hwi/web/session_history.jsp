<%@page contentType="text/html" pageEncoding="UTF-8"%>
<%@page errorPage="error_page.jsp"%>
<%@page import="org.apache.hadoop.hive.hwi.*"%>
<%@page import="org.apache.hadoop.hive.conf.*"%>
<%@page import="org.apache.hadoop.hive.ql.history.*"%>
<%@page import="org.apache.hadoop.hive.ql.history.HiveHistory.*"%>
<%@page import="java.util.*"%>

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
<% HiveHistoryViewer hv = si.getHistoryViewer(); %>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Session History</title>
  </head>
  <body>
    <table>
	  <tr>
		<td valign="top"><jsp:include page="left_navigation.jsp" /></td>
		<td valign="top">
		<h2>Hive History</h2>
		SessionID: <%= hv.getSessionId() %><br>
		<% for (String jobKey: hv.getJobInfoMap().keySet() ){ %> <%=jobKey%><br>
		<ul>
			<% QueryInfo qi = hv.getJobInfoMap().get(jobKey); %>
			<% for (String qiKey: qi.hm.keySet() ){ %>
			<li><%=qiKey%> <%=qi.hm.get(qiKey) %></li>
			<% } %>
		</ul>
		<% } %>
		<br>
		<br>
		<% for (String taskKey: hv.getTaskInfoMap().keySet() ){ %> <%=taskKey%><br>
		<ul>
			<% TaskInfo ti = hv.getTaskInfoMap().get(taskKey); %>
			<% for (String tiKey: ti.hm.keySet() ) { %>
				<li><%=tiKey%> <%=ti.hm.get(tiKey)%>
					<% if (tiKey.equalsIgnoreCase("TASK_HADOOP_ID") ){ %>
						<a href="http://<%=si.getJobTrackerURL(ti.hm.get(tiKey))%>"><%=ti.hm.get(tiKey)%></a>	 
					<% } %>		
				<% } %>
				</li>          
			<% } %>
		</ul>
		
		</td>
	  </tr>
    </table>
  </body>
</html>
