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
package org.apache.hama.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.log.LogLevel;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPMaster;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.FilterMapping;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.mortbay.util.MultiException;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal is
 * to serve up status information for the server. There are three contexts:
 * "/logs/" -> points to the log directory "/static/" -> points to common static
 * files (src/main/webapp/static) "/" -> the jsp server code from
 * (src/main/webapp/<name>)
 */
public class HttpServer {
  public static final Log LOG = LogFactory.getLog(HttpServer.class);

  static final String FILTER_INITIALIZER_PROPERTY = "hama.http.filter.initializers";

  protected final Server webServer;
  protected final Connector listener;
  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final Map<Context, Boolean> defaultContexts = new HashMap<Context, Boolean>();
  protected final List<String> filterNames = new ArrayList<String>();
  private static final int MAX_RETRIES = 10;

  /** Same as this(name, bindAddress, port, findPort, null); */
  public HttpServer(String name, String bindAddress, int port, boolean findPort)
      throws IOException {
    this(name, bindAddress, port, findPort, new Configuration());
  }

  /**
   * Create a status server on the given port. The jsp scripts are taken from
   * src/main/webapp/<name>.
   * 
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   *          increment by 1 until it finds a free port.
   * @param conf Configuration
   */
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf) throws IOException {
    webServer = new Server();
    this.findPort = findPort;

    listener = createBaseListener(conf);
    listener.setHost(bindAddress);
    listener.setPort(port);
    webServer.addConnector(listener);

    webServer.setThreadPool(new QueuedThreadPool());

    final String appDir = getWebAppsPath();
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    webServer.setHandler(contexts);
    webAppContext = new WebAppContext();

    System.setProperty("java.naming.factory.initial",
        "org.mortbay.naming.InitialContextFactory");
    System.setProperty("java.naming.factory.url.pkgs", "org.mortbay.naming");

    webAppContext.setContextPath("/");
    String warPath = appDir + "/" + name;
    if (!warPath.endsWith("/")) {
      warPath = warPath + "/";
    }
    webAppContext.setWar(warPath);
    webServer.addHandler(webAppContext);

    addDefaultApps(contexts, appDir);
    addDefaultServlets();
  }

  /**
   * Create a required listener for the Jetty instance listening on the port
   * provided. This wrapper and all subclasses must create at least one
   * listener.
   */
  protected Connector createBaseListener(Configuration conf) throws IOException {
    SelectChannelConnector ret = new SelectChannelConnector();
    ret.setLowResourceMaxIdleTime(10000);
    ret.setAcceptQueueSize(128);
    ret.setResolveNames(false);
    ret.setUseDirectBuffers(false);
    return ret;
  }

  /**
   * Add default apps.
   * 
   * @param appDir The application directory
   * @throws IOException
   */
  protected void addDefaultApps(ContextHandlerCollection parent,
      final String appDir) throws IOException {
    // set up the context for "/logs/" if "hama.log.dir" property is defined.
    String logDir = System.getProperty("hama.log.dir");
    if (logDir != null) {
      Context logContext = new Context(parent, "/logs");
      logContext.setResourceBase(logDir);
      logContext.addServlet(DefaultServlet.class, "/");
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    Context staticContext = new Context(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    defaultContexts.put(staticContext, true);
  }

  /**
   * Add default servlets.
   */
  protected void addDefaultServlets() {
    // set up default servlets
    addServlet("stacks", "/stacks", StackServlet.class);
    addServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
  }

  public void addContext(Context ctxt, boolean isFiltered) throws IOException {
    webServer.addHandler(ctxt);
    defaultContexts.put(ctxt, isFiltered);
  }

  /**
   * Add a context
   * 
   * @param pathSpec The path spec for the context
   * @param dir The directory containing the context
   * @param isFiltered if true, the servlet is added to the filter path mapping
   * @throws IOException
   */
  protected void addContext(String pathSpec, String dir, boolean isFiltered)
      throws IOException {
    if (0 == webServer.getHandlers().length) {
      throw new RuntimeException("Couldn't find handler");
    }
    WebAppContext webAppCtx = new WebAppContext();
    webAppCtx.setContextPath(pathSpec);
    webAppCtx.setWar(dir);
    addContext(webAppCtx, isFiltered);
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * 
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Add a servlet in the server.
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addInternalServlet(name, pathSpec, clazz);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Add an internal servlet in the server.
   * 
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   * @deprecated this is a temporary method
   */
  @Deprecated
  public void addInternalServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    webAppContext.addServlet(holder, pathSpec);
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  protected void defineFilter(Context ctx, String name, String classname,
      Map<String, String> parameters, String[] urls) {

    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    holder.setInitParameters(parameters);
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(Handler.ALL);
    fmap.setFilterName(name);
    ServletHandler handler = ctx.getServletHandler();
    handler.addFilter(holder, fmap);
  }

  /**
   * Add the path spec to the filter path mapping.
   * 
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(String pathSpec, Context webAppCtx) {
    ServletHandler handler = webAppCtx.getServletHandler();
    for (String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(Handler.ALL);
      handler.addFilterMapping(fmap);
    }
  }

  /**
   * Get the value in the webapp context.
   * 
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the webapps files.
   * 
   * @return the pathname as a URL
   * @throws IOException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected String getWebAppsPath() throws IOException {
    URL url = BSPMaster.class.getClassLoader().getResource("webapp");
    if (url == null)
      throw new IOException("webapps not found in CLASSPATH");
    return url.toString();
  }

  /**
   * Get the port that the server is on
   * 
   * @return the port
   */
  public int getPort() {
    return webServer.getConnectors()[0].getLocalPort();
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(int min, int max) {
    QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  /**
   * Configure an ssl listener on the server.
   * 
   * @param addr address to listen on
   * @param keystore location of the keystore
   * @param storPass password for the keystore
   * @param keyPass password for the key
   * @deprecated Use
   *             {@link #addSslListener(InetSocketAddress, Configuration, boolean)}
   */
  @Deprecated
  public void addSslListener(InetSocketAddress addr, String keystore,
      String storPass, String keyPass) throws IOException {
    if (webServer.isStarted()) {
      throw new IOException("Failed to add ssl listener");
    }
    SslSocketConnector sslListener = new SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(keystore);
    sslListener.setPassword(storPass);
    sslListener.setKeyPassword(keyPass);
    webServer.addConnector(sslListener);
  }

  /**
   * Configure an ssl listener on the server.
   * 
   * @param addr address to listen on
   * @param sslConf conf to retrieve ssl options
   * @param needClientAuth whether client authentication is required
   */
  public void addSslListener(InetSocketAddress addr, Configuration sslConf,
      boolean needClientAuth) throws IOException {
    if (webServer.isStarted()) {
      throw new IOException("Failed to add ssl listener");
    }
    if (needClientAuth) {
      // setting up SSL truststore for authenticating clients
      System.setProperty("javax.net.ssl.trustStore", sslConf.get(
          "ssl.server.truststore.location", ""));
      System.setProperty("javax.net.ssl.trustStorePassword", sslConf.get(
          "ssl.server.truststore.password", ""));
      System.setProperty("javax.net.ssl.trustStoreType", sslConf.get(
          "ssl.server.truststore.type", "jks"));
    }
    SslSocketConnector sslListener = new SslSocketConnector();
    sslListener.setHost(addr.getHostName());
    sslListener.setPort(addr.getPort());
    sslListener.setKeystore(sslConf.get("ssl.server.keystore.location"));
    sslListener.setPassword(sslConf.get("ssl.server.keystore.password", ""));
    sslListener.setKeyPassword(sslConf.get("ssl.server.keystore.keypassword",
        ""));
    sslListener.setKeystoreType(sslConf.get("ssl.server.keystore.type", "jks"));
    sslListener.setNeedClientAuth(needClientAuth);
    webServer.addConnector(sslListener);
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      int port;
      int oriPort = listener.getPort(); // The original requested port
      while (true) {
        try {
          port = webServer.getConnectors()[0].getLocalPort();
          LOG.info("Port returned by webServer.getConnectors()[0]."
              + "getLocalPort() before open() is " + port
              + ". Opening the listener on " + oriPort);
          listener.open();
          port = listener.getLocalPort();
          LOG.info("listener.getLocalPort() returned "
              + listener.getLocalPort()
              + " webServer.getConnectors()[0].getLocalPort() returned "
              + webServer.getConnectors()[0].getLocalPort());
          // Workaround to handle the problem reported in HADOOP-4744
          if (port < 0) {
            Thread.sleep(100);
            int numRetries = 1;
            while (port < 0) {
              LOG.warn("listener.getLocalPort returned " + port);
              if (numRetries++ > MAX_RETRIES) {
                throw new Exception(" listener.getLocalPort is returning "
                    + "less than 0 even after " + numRetries + " resets");
              }
              for (int i = 0; i < 2; i++) {
                LOG.info("Retrying listener.getLocalPort()");
                port = listener.getLocalPort();
                if (port > 0) {
                  break;
                }
                Thread.sleep(200);
              }
              if (port > 0) {
                break;
              }
              LOG.info("Bouncing the listener");
              listener.close();
              Thread.sleep(1000);
              listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
              listener.open();
              Thread.sleep(100);
              port = listener.getLocalPort();
            }
          } // Workaround end
          LOG.info("Jetty bound to port " + port);
          webServer.start();
          // Workaround for HADOOP-6386
          port = listener.getLocalPort();
          if (port < 0) {
            LOG.warn("Bounds port is " + port + " after webserver start");
            for (int i = 0; i < MAX_RETRIES / 2; i++) {
              try {
                webServer.stop();
              } catch (Exception e) {
                LOG.warn("Can't stop  web-server", e);
              }
              Thread.sleep(1000);

              listener.setPort(oriPort == 0 ? 0 : (oriPort += 1));
              listener.open();
              Thread.sleep(100);
              webServer.start();
              LOG.info(i + "attempts to restart webserver");
              port = listener.getLocalPort();
              if (port > 0)
                break;
            }
            if (port < 0)
              throw new Exception("listener.getLocalPort() is returning "
                  + "less than 0 even after " + MAX_RETRIES + " resets");
          }
          // End of HADOOP-6386 workaround
          break;
        } catch (IOException ex) {
          // if this is a bind exception,
          // then try the next port number.
          if (ex instanceof BindException) {
            if (!findPort) {
              throw (BindException) ex;
            }
          } else {
            LOG.info("HttpServer.start() threw a non Bind IOException");
            throw ex;
          }
        } catch (MultiException ex) {
          LOG.info("HttpServer.start() threw a MultiException");
          throw ex;
        }
        listener.setPort((oriPort += 1));
      }
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException("Problem starting http server", e);
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    listener.close();
    webServer.stop();
  }

  public void join() throws InterruptedException {
    webServer.join();
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException {

      PrintWriter out = new PrintWriter(response.getOutputStream());
      ReflectionUtils.printThreadInfo(out, "");
      out.close();
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }
  }
}
