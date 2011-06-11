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
<%@ page contentType="text/html; charset=UTF-8" import="javax.servlet.*"
	import="javax.servlet.http.*" import="java.io.*" import="java.util.*"
	import="java.text.DecimalFormat" import="org.apache.hama.bsp.*"
	import="org.apache.hama.util.*"%>
<%!private static final long serialVersionUID = 1L;%>
<%
	BSPMaster tracker = (BSPMaster) application
			.getAttribute("bsp.master");
	ClusterStatus status = tracker.getClusterStatus(true);
	String trackerName = tracker.getBSPMasterName();
	String type = request.getParameter("type");
%>
<%!public void generateGroomsTable(JspWriter out, String type,
			ClusterStatus status, BSPMaster master) throws IOException {
	out.print("<center>\n");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    out.print("<tr><td align=\"center\" colspan=\"6\"><b>Groom Servers</b></td></tr>\n");
    out.print("<tr><td><b>Name</b></td>" + "<td><b>Host</b></td>"
        + "<td><b># running tasks</b></td></tr>\n");
    for (Map.Entry<String, String> entry : status.getActiveGroomNames()
        .entrySet()) {
      out.print("<tr><td><a href=\"http://");
      out.print(entry.getKey() + ":" + master.getHttpPort() + "/\">");
      out.print(entry.getValue() + "</a></td><td>");
      out.print(entry.getValue() + "</td>" + "<td>" + 1 + "</td></tr>\n");
    }
    out.print("</table>\n");
    out.print("</center>\n");
  }%>

<html>

<title><%=trackerName%> Hama Machine List</title>

<body>
<h1><a href="bspmaster.jsp"><%=trackerName%></a> Hama Machine List</h1>

<h2>Grooms</h2>
<%
  generateGroomsTable(out, type, status, tracker);
%>

<%
  out.println(BSPServletUtil.htmlFooter());
%>