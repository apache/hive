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
<%@page errorPage="error_page.jsp" %>
<%@ page import="org.apache.hadoop.hive.hwi.*,java.io.*" %>
<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>
<% String sessionName=request.getParameter("sessionName"); %>
<% HWISessionItem sess = hs.findSessionItemByName(auth,sessionName);	%>
<% int start=0; 
   if (request.getParameter("start")!=null){
     start = Integer.parseInt( request.getParameter("start") );
   }
%>
<% int bsize=1024; 
   if (request.getParameter("bsize")!=null){
     bsize = Integer.parseInt( request.getParameter("bsize") );
   }
%>

<html>
  <head>
    <title>Hive Web Interface</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  <jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Hive Web Interface</h2>
          <p><%=sess.getResultFile() %></p>
          <textarea rows="8" cols="40"><%   
			  File f = new File(   sess.getResultFile()  ); 
			  BufferedReader br = new BufferedReader( new FileReader(f) );
			  br.skip(start*bsize);
			  
			  char [] c = new char [bsize] ;
			  int cread=-1;
			  
			  if( ( cread=br.read(c)) != -1 ){
			   out.println( c ); 
			  }
			  br.close();	  
			%></textarea>
          <br>
          <% long numberOfBlocks = f.length()/ (long)bsize;%>
          This file contains <%=numberOfBlocks%> of <%=bsize%> blocks.  
        
          <a href="/hwi/view_file.jsp?sessionName=<%=sessionName%>&start=<%=(start+1) %>&bsize=<%=bsize %>">Next Block</a>
        </td>
      </tr>
    </table>
  </body>
</html>
