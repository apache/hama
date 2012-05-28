<!--
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
-->

<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hama.bsp.*"
  import="org.apache.hama.util.*"
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%
  GroomServer groom = (GroomServer) application.getAttribute("groom.server");
  String groomName = groom.getGroomServerName();
%>

<html>

<title><%= groomName %> - Server Status</title>
<body>
<h2><%= groomName %></h2>
<hr>
<h2>Running tasks</h2>
<table border=2 cellpadding="5" cellspacing="2">
<tr><td align="center">Task Attempts</td><td>Status</td>
    </tr>
  <%
     Iterator itr = groom.getRunningTaskStatuses().iterator();
     while (itr.hasNext()) {
       TaskStatus status = (TaskStatus) itr.next();
       out.print("<tr><td>" + status.getTaskId());
       out.print("</td><td>" + status.getRunState()); 
       out.print("</td></tr>\n");
     }
  %>
</table>
<hr>
<h2>Local Logs</h2>
<a href="/logs/">Log</a> directory

<%
  out.println(BSPServletUtil.htmlFooter());
%>