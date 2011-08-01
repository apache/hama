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
  String idString = request.getParameter("jobid");
  JobStatus status = tracker.getJobStatus(BSPJobID.forName(idString));
  JobStatus.State state = status.getState();
%>

<html>

<title>Hama BSP Job Summary</title>

<body>
  <h1><%=status.getName()%></h1>

  <b>State: </b>  <%=state.toString() %> 
  
  <br/> <br/>
  <table border="1" cellpadding="5" cellspacing="0">
    <tr>
      <th>Name</th>
      <th>User</th>
      <th>SuperStep</th>
      <th>StartTime</th>
      <th>FinishTime</th>
    </tr>

    <tr>
      <td><%=status.getName() %></td>
      <td><%=status.getUsername() %></td>
      <td><%=status.getSuperstepCount() %></td>
      <td><%=new Date(status.getStartTime()).toString() %></td>
      <td>
        <% if(status.getFinishTime() != 0L) {out.write(new Date(status.getFinishTime()).toString());} %>
      </td>
    </tr>

  </table>
  
  <hr>
  <a href="bspmaster.jsp">Back to BSPMaster</a>

  <%
    out.println(BSPServletUtil.htmlFooter());
  %>