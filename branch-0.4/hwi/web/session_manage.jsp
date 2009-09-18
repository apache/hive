<%@page import="org.apache.hadoop.hive.hwi.*" %>
<%@page errorPage="error_page.jsp" %>
<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs");; %>

<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>
<% String sessionName=request.getParameter("sessionName"); %>
<% HWISessionItem sess = hs.findSessionItemByName(auth,sessionName); %>
<% String message=null; %>
<% 
	String errorFile=request.getParameter("errorFile");
	String resultFile=request.getParameter("resultFile");
	String query = request.getParameter("query");
	String silent = request.getParameter("silent");
	String start = request.getParameter("start");
%>
<% 
  if (request.getParameter("start")!=null ){ 
    if ( sess.getStatus()!=HWISessionItem.WebSessionItemStatus.QUERY_RUNNING){
      sess.setErrorFile(errorFile);
	  sess.setResultFile(resultFile);
	  sess.setQuery(query);
	  
	  if (query.length()==0){
		  message="You did not specify a query";
		  start="NO";
	  }
	  
	  if (silent.equalsIgnoreCase("YES") )
		sess.setSSIsSilent(true);
	  else
		sess.setSSIsSilent(false);
		   
	  message="Changes accepted.";
	  if (start.equalsIgnoreCase("YES") ){
	    sess.clientStart();
		message="Session is set to start.";
	  }
	}
  } 
%>

<html>
  <head>
    <title>Manage Session <%=sessionName%></title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  		<jsp:include page="/left_navigation.jsp"/>
	  	</td>
        <td valign="top">
          <h2>Manage Session <%=sessionName%></h2>
          
          <% if (message != null) {  %> <font color="red"><%=message %></font> <% } %>
          <br>
          <% if (sess.getStatus()==HWISessionItem.WebSessionItemStatus.QUERY_RUNNING) { %>
          	<font color="RED">Session is in QUERY_RUNNING state. Changes are not possible!</font>
          <% } %>
          <br>  
          
          <% if (sess.getStatus()==HWISessionItem.WebSessionItemStatus.QUERY_RUNNING){ %>
          	<%-- 
          	View JobTracker: <a href="<%= sess.getJobTrackerURI() %>">View Job</a><br>
          	Kill Command: <%= sess.getKillCommand() %>
          	 Session Kill: <a href="/hwi/session_kill.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br>
          	--%>
          <% } %>
          
          Session History:  <a href="/hwi/session_history.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br> 
          Session Diagnostics: <a href="/hwi/session_diagnostics.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br>
		  Set Processor: <a href="/hwi/set_processor.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br> 
          Session Remove: <a href="/hwi/session_remove.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br> 
          <br>
          
			<form action="session_manage.jsp">
				<input type="hidden" name="sessionName" value="<%=sessionName %>">
			<table>
				<tr>
				<td>Result File</td>
				<td>
				<input type="text" name="resultFile" value="<% 
				if (sess.getResultFile()==null) { out.print(""); } else { out.print(sess.getResultFile()); }
				%>"><br>
				<% if (sess.getResultFile()!=null) { %>
					<a href="/hwi/view_file.jsp?sessionName=<%=sessionName%>">View File</a>
				<% } %>
				</td>
				</tr>
				
				<tr>
				<td>Error File</td>
				<td>
				<input type="text" name="errorFile" value="<% 
				if (sess.getErrorFile()==null) { out.print(""); } else { out.print(sess.getErrorFile()); }
				%>"></td>
				</tr>
				
				<tr>
					<td>Query</td>
					<td><textarea name="query" rows="8" cols="70"><% 
				if (sess.getQuery()==null) { out.print(""); } else { out.print(sess.getQuery()); }
				%></textarea></td>
				</tr>
				
				<tr>
				<td>Silent Mode ?</td>
				<td><select name="silent">
					<option value="YES" 
					<% if (sess.getSSIsSilent()==true) { out.print("SELECTED=\"TRUE\""); } %>
					>YES</option>
					<option value="NO"
					<% if (sess.getSSIsSilent()==false) { out.print("SELECTED=\"TRUE\""); } %>
					>NO</option>
					</select>
				</td>
				</tr>
				
				<tr>
					<td>Start Query?</td>
					<td><select name="start">
						<option value="NO" SELECTED="TRUE">NO</option>
						<option value="YES" >YES</option>
						</select>
					</td>
				</tr>
					
				<tr>
					<td>Query Return Code</td>
					<td><%=sess.getQueryRet() %></td>
				</tr>
				<tr>
					<td colSpan="2">
					  <% if (sess.getStatus()==HWISessionItem.WebSessionItemStatus.QUERY_RUNNING) { %>
			          		<font color="RED">Session is in QUERY_RUNNING state. Changes are not possible!</font>
			          <% } else { %>
			          	<input type="submit">
			          <% } %>
					</td>
				</tr>
				</table>	
			</form>
        </td>
      </tr>
    </table>
  </body>
</html>
