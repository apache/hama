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
  JobStatus[] runningJobs = tracker.jobsToComplete();
  JobStatus[] allJobs = tracker.getAllJobs();
%>
<%!private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
 
  public void generateSummaryTable(JspWriter out, ClusterStatus status,
      BSPMaster tracker) throws IOException {
    String tasksPerNode = status.getGroomServers() > 0 ? percentFormat
        .format(((double) (status.getMaxTasks()) / status
            .getGroomServers())) : "-";
    out.print("<table border=\"1\" cellpadding=\"5\" cellspacing=\"0\">\n"
        + "<tr>" + "<th>Groom Servers</th><th>BSP Task Capacity</th>"
        + "<th>Avg. Tasks/Node</th>"
        + "<th>Blacklisted Nodes</th></tr>\n");
    out.print("<tr><td><a href=\"machines.jsp?type=active\">"
        + status.getActiveGroomNames().size() + "</a></td><td>"
        + status.getMaxTasks() + "</td><td>" + tasksPerNode
        + "</td><td><a href=\"machines.jsp?type=blacklisted\">" + 0
        + "</a>" + "</td></tr></table>\n");

    out.print("<br>");
  }%>


<html>
<head>
<title><%=trackerName%> Hama BSP Administration</title>
<!--  <link rel="stylesheet" type="text/css" href="/static/hadoop.css">-->
</head>
<body>

<h1><%=trackerName%> Hama BSP Administration</h1>

<b>State:</b>
<%=status.getBSPMasterState()%><br>
<b>Started:</b>
<%=new Date(tracker.getStartTime())%><br>
<b>Identifier:</b>
<%=tracker.getBSPMasterIdentifier()%><br>

<hr>
<%
  generateSummaryTable(out, status, tracker);
%>
<hr />

<h2 id="running_jobs">Running Jobs</h2>
<%=BSPServletUtil.generateJobTable("Running", runningJobs,
          30, 0)%>
<hr> 
<h2 id="running_jobs">All Jobs History</h2>
<%=BSPServletUtil.generateJobTable("All", allJobs,
          30, 0)%>
<%
  out.println(BSPServletUtil.htmlFooter());
%>