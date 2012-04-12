<%--
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
--%>
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
    if ( sess.getStatus()==HWISessionItem.WebSessionItemStatus.READY){
      sess.setErrorFile(errorFile);
      sess.setResultFile(resultFile);
      sess.clearQueries();
      for (String q : query.split(";") ){
        sess.addQuery(q);
      }
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
<!DOCTYPE html>
<html>
<head>
<title>Manage Session <%=sessionName%></title>
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
				<h2>
					Manage Session
					<%=sessionName%></h2>

				<% if (message != null) {  %>
				<div class="alert alert-info"><%=message %></div>
				<% } %>

				<% if (sess.getStatus()==HWISessionItem.WebSessionItemStatus.QUERY_RUNNING) { %>
				<div class="alert alert-warning">Session is in QUERY_RUNNING
					state. Changes are not possible!</div>
				<% } %>

				<% if (sess.getStatus()==HWISessionItem.WebSessionItemStatus.QUERY_RUNNING){ %>
				<%-- 
          	View JobTracker: <a href="<%= sess.getJobTrackerURI() %>">View Job</a><br>
          	Kill Command: <%= sess.getKillCommand() %>
          	 Session Kill: <a href="/hwi/session_kill.jsp?sessionName=<%=sessionName%>"><%=sessionName%></a><br>
          	--%>
				<% } %>

				<div class="btn-group">
					<a class="btn" href="/hwi/session_history.jsp?sessionName=<%=sessionName%>"><i class="icon-book"></i> History</a>
					<a class="btn" href="/hwi/session_diagnostics.jsp?sessionName=<%=sessionName%>"><i class="icon-cog"></i> Diagnostics</a>
					<a class="btn"href="/hwi/session_remove.jsp?sessionName=<%=sessionName%>"><i class="icon-remove"></i> Remove</a>
					<a class="btn"href="/hwi/session_result.jsp?sessionName=<%=sessionName%>"><i class=" icon-download-alt"></i> Result Bucket</a>
				</div>

				<form action="session_manage.jsp" class="form-horizontal">
					<input type="hidden" name="sessionName" value="<%=sessionName %>">

					<fieldset>
						<legend>Session Details	</legend>
						<div class="control-group">
							<label class="control-label" for="fldresfile">Result File</label>
							<div class="controls">
								<input id="fldresfile" type="text" name="resultFile"
									value="<%
                    if (sess.getResultFile()==null) { out.print(""); } else { out.print(sess.getResultFile()); }
                 %>">
								<% if (sess.getResultFile()!=null) { %>
								<a href="/hwi/view_file.jsp?sessionName=<%=sessionName%>">View
									File</a>
								<% } %>
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="flderrfile">Error File</label>
							<div class="controls">
								<input id="flderrfile" type="text" name="errorFile"
									value="<%
                    if (sess.getErrorFile()==null) { out.print(""); } else { out.print(sess.getErrorFile()); }
                 %>">
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="fldquery">Query</label>
							<div class="controls">
								<textarea id="fldquery" name="query" rows="8" cols="70">
									<% 
				if (sess.getQueries()==null) { 
				  out.print(""); 
				} else { 
			          for (String qu: sess.getQueries() ) {
				    out.print(qu); out.print(" ; ");
				  }	
				}
				%>
								</textarea>
							</div>
						</div>


						<div class="control-group">
							<label class="control-label" for="fldsilent">Silent Mode</label>
							<div class="controls">
								<select id="fldsilent" name="silent">
									<option value="YES"
										<% if (sess.getSSIsSilent()==true) { out.print("SELECTED=\"TRUE\""); } %>>YES</option>
									<option value="NO"
										<% if (sess.getSSIsSilent()==false) { out.print("SELECTED=\"TRUE\""); } %>>NO</option>
								</select>
							</div>
						</div>

						<div class="control-group">
							<label class="control-label" for="fldstart">Start Query</label>
							<div class="controls">
								<select id="fldstart" name="start">
									<option value="NO" SELECTED="TRUE">NO</option>
									<option value="YES">YES</option>
								</select>
							</div>
						</div>

					</fieldset>

					<h3>Query Return Codes</h3>
					<p>
						<% for (int i=0; i< sess.getQueryRet().size();++i ){ %>
						<%=i%>
						:
						<%=sess.getQueryRet().get(i)%><br>
						<% } %>
					</p>

					<% if (sess.getStatus()!=HWISessionItem.WebSessionItemStatus.QUERY_RUNNING) { %>
					<div class="form-actions">
						<button type="submit" class="btn btn-primary">Submit</button>
					</div>

					<% } %>
				</form>
			</div><!-- span8 -->
		</div><!-- row -->
	</div><!-- container -->
</body>
</html>
