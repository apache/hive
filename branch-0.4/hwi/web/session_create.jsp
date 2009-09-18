<%@ page import="org.apache.hadoop.hive.hwi.*" %>
<%@ page errorPage="error_page.jsp" %>
<% HWIAuth auth = (HWIAuth) session.getAttribute("auth"); %>
<% if (auth==null) { %>
	<jsp:forward page="/authorize.jsp" />
<% } %>

<% HWISessionManager hs = (HWISessionManager) application.getAttribute("hs"); %>

<% String sessionName=request.getParameter("sessionName"); %>
<% String message = null; %>

<% 
	if (sessionName != null){
		if (sessionName.equals("")){
			message="This is not a valid session name";
		} else {
			HWISessionItem item= hs.findSessionItemByName(auth, sessionName);
			if (item!=null){
				message="This name is already in use";
			} else {
				hs.createSession(auth,sessionName);
				RequestDispatcher rd = application.getRequestDispatcher("/session_manage.jsp");
				rd.forward(request,response);
			}
		}
	}

	if (sessionName == null){
		sessionName="";
	}
%>

<html>
  <head>
    <title>Hive Web Interface-Create a Hive Session</title>
  </head>
  <body>
    <table>
      <tr>
        <td valign="top" valign="top" width="100">
	  <jsp:include page="/left_navigation.jsp"/></td>
        <td valign="top">
          <h2>Create a Hive Session</h2>
          <form action="session_create.jsp">
            <table border="1">
            <tr>
              <td>Session Name</td>
              <td><input type="text" name="sessionName" value="<%=sessionName%>" ></td>
            </tr>
            <tr>
            	<td colSpan="2"><input type="submit"></td>
            </tr>
            </table>
          </form>
        </td>
      </tr>
    </table>
  </body>
</html>