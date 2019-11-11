/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.servlet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.commons.io.FileCleaningTracker;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.client.HttpClient;
import org.apache.lucene.util.Version;
import org.apache.solr.SolrResources.SolrResources;
import org.apache.solr.api.V2HttpCall;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.security.AuditEvent;
import org.apache.solr.security.AuditEvent.EventType;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.util.SolrFileCleaningTracker;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.apache.solr.util.tracing.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;

/**
 * This filter looks at the incoming URL maps them to handlers defined in solrconfig.xml
 *
 * @since solr 1.2
 */
public class SolrDispatchFilter extends BaseSolrFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final AtomicInteger initCalled = new AtomicInteger(0);
  // protected final CountDownLatch init = new CountDownLatch(1);

  protected volatile String abortErrorMessage = null;
  // TODO using Http2Client
  protected volatile HttpClient httpClient; // nocommit make this client?
  private volatile List<Pattern> excludePatterns;

  private boolean isV2Enabled = !"true".equals(System.getProperty("disable.v2.api", "false"));

  private final String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  private volatile boolean closeOnDestroy = true;

  private volatile SolrResources solrResources;

  /**
   * Enum to define action that needs to be processed. PASSTHROUGH: Pass through to Restlet via webapp. FORWARD: Forward
   * rewritten URI (without path prefix and core/collection name) to Restlet RETURN: Returns the control, and no further
   * specific processing is needed. This is generally when an error is set and returned. RETRY:Retry the request. In
   * cases when a core isn't found to work with, this is set.
   */
  public enum Action {
    PASSTHROUGH, FORWARD, RETURN, RETRY, ADMIN, REMOTEQUERY, PROCESS
  }

  public SolrDispatchFilter() {
  }

  public static final String PROPERTIES_ATTRIBUTE = "solr.properties";

  public static final String SOLRHOME_ATTRIBUTE = "solr.solr.home";

  public static final String SOLR_INSTALL_DIR_ATTRIBUTE = "solr.install.dir";

  public static final String SOLR_DEFAULT_CONFDIR_ATTRIBUTE = "solr.default.confdir";

  public static final String SOLR_LOG_MUTECONSOLE = "solr.log.muteconsole";

  public static final String SOLR_LOG_LEVEL = "solr.log.level";

  @Override
  public void init(FilterConfig config) throws ServletException {
    SSLConfigurationsFactory.current().init();
    log.trace("SolrDispatchFilter.init(): {}", this.getClass().getClassLoader());
    
    initCalled.incrementAndGet();
    
    if (initCalled.get() > 1) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Filter init should only be called once.");
    }

    try {

      SolrRequestParsers.fileCleaningTracker = new SolrFileCleaningTracker();

      StartupLoggingUtils.checkLogDir();
      log.info("Using logger factory {}", StartupLoggingUtils.getLoggerImplStr());
      logWelcomeBanner();
      String muteConsole = System.getProperty(SOLR_LOG_MUTECONSOLE);
      if (muteConsole != null
          && !Arrays.asList("false", "0", "off", "no").contains(muteConsole.toLowerCase(Locale.ROOT))) {
        StartupLoggingUtils.muteConsole();
      }
      String logLevel = System.getProperty(SOLR_LOG_LEVEL);
      if (logLevel != null) {
        log.info("Log level override, property solr.log.level=" + logLevel);
        StartupLoggingUtils.changeLogLevel(logLevel);
      }

      String exclude = config.getInitParameter("excludePatterns");
      if (exclude != null) {
        String[] excludeArray = exclude.split(",");
        List<Pattern> exPatterns = new ArrayList<>();
        for (String element : excludeArray) {
          exPatterns.add(Pattern.compile(element));
        }
        excludePatterns = Collections.unmodifiableList(exPatterns);
      }
      try {
        Properties extraProperties = (Properties) config.getServletContext().getAttribute(PROPERTIES_ATTRIBUTE);
        if (extraProperties == null)
          extraProperties = new Properties();

        String solrHome = (String) config.getServletContext().getAttribute(SOLRHOME_ATTRIBUTE);
        final Path solrHomePath = solrHome == null ? SolrResourceLoader.locateSolrHome() : Paths.get(solrHome);

        solrResources = new SolrResources(solrHomePath, extraProperties);

        if (log.isDebugEnabled()) log.debug("user.dir=" + System.getProperty("user.dir"));
        
        solrResources.start();
        
        // nocommit - sharble httpclient
        this.httpClient = solrResources.getCoreContainer().getUpdateShardHandler().getUpdateOnlyHttpClient().getHttpClient(); 
      } catch (Throwable t) {
        SW.propegateInterrupt("Could not start Solr. Check solr/home property and the logs", t);
      }

    } finally {

      log.trace("SolrDispatchFilter.init() done");
      // init.countDown();
    }
  }

  private void logWelcomeBanner() {
    log.info(" ___      _       Welcome to Apache Solr™ version {}", solrVersion());
    log.info("/ __| ___| |_ _   Starting in {} mode on port {}", isCloudMode() ? "cloud" : "standalone", getSolrPort());
    log.info("\\__ \\/ _ \\ | '_|  Install dir: {}", System.getProperty(SOLR_INSTALL_DIR_ATTRIBUTE));
    log.info("|___/\\___/_|_|    Start time: {}", Instant.now().toString());
  }

  private String solrVersion() {
    String specVer = Version.LATEST.toString();
    try {
      String implVer = SolrCore.class.getPackage().getImplementationVersion();
      return (specVer.equals(implVer.split(" ")[0])) ? specVer : implVer;
    } catch (Exception e) {
      if (e instanceof NullPointerException) {
        return specVer;
      }
      SW.propegateInterrupt(e);
      return specVer;
    }
  }

  private String getSolrPort() {
    return System.getProperty("jetty.port");
  }

  /* We are in cloud mode if Java option zkRun exists OR zkHost exists and is non-empty */
  private boolean isCloudMode() {
    return ((System.getProperty("zkHost") != null && !StringUtils.isEmpty(System.getProperty("zkHost")))
        || System.getProperty("zkRun") != null);
  }

  public CoreContainer getCores() {
    return solrResources.getCoreContainer();
  }

  @Override
  public void destroy() {
    log.info("Servlet destroy called");
    
    //if (closeOnDestroy) { // nocommit
      close();
    //}
  }

  public void close() {
    // nocommit httpclient?
    try (SW closer = new SW(this, true)) {
      closer.add("FilterAndSolrResources", solrResources, GlobalTracer.get(), () -> {
        FileCleaningTracker fileCleaningTracker = null;
        try {
          fileCleaningTracker = SolrRequestParsers.fileCleaningTracker;
          if (fileCleaningTracker != null) {
            fileCleaningTracker.exitWhenFinished();
          }
        } catch (NullPointerException e) {
          // okay
        } catch (Exception e) {
          throw new SW.Exp("Exception closing FileCleaningTracker", e);
        } finally {
          SolrRequestParsers.fileCleaningTracker = null;
        }
        return "FileCleaningTracker";

      });
    }
  }

  @Override
  public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
      throws IOException, ServletException {
    if (!(req instanceof HttpServletRequest)) return;
    HttpServletRequest request = closeShield((HttpServletRequest) req);
    HttpServletResponse response = closeShield((HttpServletResponse) resp); 
    doFilter(request, response, chain, false);
  }

  public void doFilter(HttpServletRequest request, HttpServletResponse response, FilterChain chain, boolean retry)
      throws IOException, ServletException {
    Scope scope = null;
    Span span = null;
    try {
      SolrResources.waitForClose();

      String requestPath = request.getServletPath();
      // No need to even create the HttpSolrCall object if this path is excluded.
      if (excludePatterns != null) {
        String extraPath = request.getPathInfo();
        if (extraPath != null) {
          // In embedded mode, servlet path is empty - include all post-context path here for testing
          requestPath += extraPath;
        }
        for (Pattern p : excludePatterns) {
          Matcher matcher = p.matcher(requestPath);
          if (matcher.lookingAt()) {
            chain.doFilter(request, response);
            return;
          }
        }
      }

      SpanContext parentSpan = GlobalTracer.get().extract(request);
      Tracer tracer = GlobalTracer.getTracer();

      Tracer.SpanBuilder spanBuilder = null;
      String hostAndPort = request.getServerName() + "_" + request.getServerPort();
      if (parentSpan == null) {
        spanBuilder = tracer.buildSpan(request.getMethod() + ":" + hostAndPort);
      } else {
        spanBuilder = tracer.buildSpan(request.getMethod() + ":" + hostAndPort)
            .asChildOf(parentSpan);
      }

      spanBuilder
          .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER)
          .withTag(Tags.HTTP_URL.getKey(), request.getRequestURL().toString());
      span = spanBuilder.start();
      scope = tracer.scopeManager().activate(span);

      AtomicReference<HttpServletRequest> wrappedRequest = new AtomicReference<>();
      if (!authenticateRequest(request, response, wrappedRequest)) { // the response and status code have already been
                                                                     // sent
        return;
      }
      if (wrappedRequest.get() != null) {
        request = wrappedRequest.get();
      }

      HttpSolrCall call = getHttpSolrCall(request, response, retry);
      ExecutorUtil.setServerThreadFlag(Boolean.TRUE);
      try {
        Action result = call.call();
        switch (result) {
          case PASSTHROUGH:
            chain.doFilter(request, response);
            break;
          case RETRY:
            doFilter(request, response, chain, true); // RECURSION
            break;
          case FORWARD:
            request.getRequestDispatcher(call.getPath()).forward(request, response);
            break;
          case ADMIN:
          case PROCESS:
          case REMOTEQUERY:
          case RETURN:
            break;
        }
      } finally {
        call.destroy();
        ExecutorUtil.setServerThreadFlag(null);
      }
    } finally {
      if (span != null) span.finish();
      if (scope != null) scope.close();

      GlobalTracer.get().clearContext();
      consumeInputFully(request);
    }
  }

  // we make sure we read the full client request so that the client does
  // not hit a connection reset and we can reuse the
  // connection - see SOLR-8453 and SOLR-8683
  private void consumeInputFully(HttpServletRequest req) {
    try {
      ServletInputStream is = req.getInputStream();
      while (!is.isFinished() && is.read() != -1) {
      }
    } catch (IOException e) {
      log.info("Could not consume full client request", e);
    }
  }

  /**
   * Allow a subclass to modify the HttpSolrCall. In particular, subclasses may want to add attributes to the request
   * and send errors differently
   */
  protected HttpSolrCall getHttpSolrCall(HttpServletRequest request, HttpServletResponse response, boolean retry) {
    String path = request.getServletPath();
    if (request.getPathInfo() != null) {
      // this lets you handle /update/commit when /update is a servlet
      path += request.getPathInfo();
    }

    if (isV2Enabled && (path.startsWith("/____v2/") || path.equals("/____v2"))) {
      return new V2HttpCall(this, solrResources.getCoreContainer(), request, response, false);
    } else {
      return new HttpSolrCall(this, solrResources.getCoreContainer(), request, response, retry);
    }
  }

  private boolean authenticateRequest(HttpServletRequest request, HttpServletResponse response,
      final AtomicReference<HttpServletRequest> wrappedRequest) throws IOException {
    boolean requestContinues = false;
    final AtomicBoolean isAuthenticated = new AtomicBoolean(false);
    AuthenticationPlugin authenticationPlugin = solrResources.getCoreContainer().getAuthenticationPlugin();
    if (authenticationPlugin == null) {
      if (shouldAudit(EventType.ANONYMOUS)) {
        solrResources.getCoreContainer().getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.ANONYMOUS, request));
      }
      return true;
    } else {
      // /admin/info/key must be always open. see SOLR-9188
      // tests work only w/ getPathInfo
      // otherwise it's just enough to have getServletPath()
      String requestPath = request.getPathInfo();
      if (requestPath == null)
        requestPath = request.getServletPath();
      if (PublicKeyHandler.PATH.equals(requestPath)) {
        if (log.isDebugEnabled())
          log.debug("Pass through PKI authentication endpoint");
        return true;
      }
      // /solr/ (Admin UI) must be always open to allow displaying Admin UI with login page
      if ("/solr/".equals(requestPath) || "/".equals(requestPath)) {
        if (log.isDebugEnabled())
          log.debug("Pass through Admin UI entry point");
        return true;
      }
      String header = request.getHeader(PKIAuthenticationPlugin.HEADER);
      if (header != null && solrResources.getCoreContainer().getPkiAuthenticationPlugin() != null)
        authenticationPlugin = solrResources.getCoreContainer().getPkiAuthenticationPlugin();
      try {
        if (log.isDebugEnabled()) log.debug("Request to authenticate: {}, domain: {}, port: {}", request, request.getLocalName(),
            request.getLocalPort());
        // upon successful authentication, this should call the chain's next filter.
        requestContinues = authenticationPlugin.authenticate(request, response, (req, rsp) -> {
          isAuthenticated.set(true);
          wrappedRequest.set((HttpServletRequest) req);
        });
      } catch (Exception e) {
        throw new SW.Exp("Error during request authentication", e);
      }
    }
    // requestContinues is an optional short circuit, thus we still need to check isAuthenticated.
    // This is because the AuthenticationPlugin doesn't always have enough information to determine if
    // it should short circuit, e.g. the Kerberos Authentication Filter will send an error and not
    // call later filters in chain, but doesn't throw an exception. We could force each Plugin
    // to implement isAuthenticated to simplify the check here, but that just moves the complexity to
    // multiple code paths.
    if (!requestContinues || !isAuthenticated.get()) {
      response.flushBuffer();
      if (shouldAudit(EventType.REJECTED)) {
        solrResources.getCoreContainer().getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.REJECTED, request));
      }
      return false;
    }
    if (shouldAudit(EventType.AUTHENTICATED)) {
      solrResources.getCoreContainer().getAuditLoggerPlugin().doAudit(new AuditEvent(EventType.AUTHENTICATED, request));
    }
    return true;
  }

  public static class ClosedServletInputStream extends ServletInputStream {

    public static final ClosedServletInputStream CLOSED_SERVLET_INPUT_STREAM = new ClosedServletInputStream();

    @Override
    public int read() {
      return -1;
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setReadListener(ReadListener arg0) {
    }
  }

  public static class ClosedServletOutputStream extends ServletOutputStream {

    public static final ClosedServletOutputStream CLOSED_SERVLET_OUTPUT_STREAM = new ClosedServletOutputStream();

    @Override
    public void write(final int b) throws IOException {
      throw new IOException("write(" + b + ") failed: stream is closed");
    }

    @Override
    public void flush() throws IOException {
      throw new IOException("flush() failed: stream is closed");
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setWriteListener(WriteListener arg0) {
      throw new RuntimeException("setWriteListener() failed: stream is closed");
    }
  }

  private static String CLOSE_STREAM_MSG = "Attempted close of http request or response stream - in general you should not do this, "
      + "you may spoil connection reuse and possibly disrupt a client. If you must close without actually needing to close, "
      + "use a CloseShield*Stream. Closing or flushing the response stream commits the response and prevents us from modifying it. "
      + "Closing the request stream prevents us from gauranteeing ourselves that streams are fully read for proper connection reuse."
      + "Let the container manage the lifecycle of these streams when possible.";

  /**
   * Check if audit logging is enabled and should happen for given event type
   * 
   * @param eventType
   *          the audit event
   */
  private boolean shouldAudit(AuditEvent.EventType eventType) {
    return solrResources.getCoreContainer().getAuditLoggerPlugin() != null
        && solrResources.getCoreContainer().getAuditLoggerPlugin().shouldLog(eventType);
  }

  /**
   * Wrap the request's input stream with a close shield. If this is a retry, we will assume that the stream has already
   * been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet output stream.
   *
   * @param request
   *          The request to wrap.
   *          If this is an original request or a retry.
   * @return A request object with an {@link InputStream} that will ignore calls to close.
   */
  public static HttpServletRequest closeShield(HttpServletRequest request) {

    return new HttpServletRequestWrapper(request) {

      @Override
      public ServletInputStream getInputStream() throws IOException {

        return new ServletInputStreamWrapper(super.getInputStream()) {
          @Override
          public void close() {
            // even though we skip closes, we let local tests know not to close so that a full understanding can take
            // place
            assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                "org\\.apache\\.(?:solr|lucene).*") ? false : true : CLOSE_STREAM_MSG;
            this.stream = ClosedServletInputStream.CLOSED_SERVLET_INPUT_STREAM;
          }
        };

      }
    };

  }

  /**
   * Wrap the response's output stream with a close shield. If this is a retry, we will assume that the stream has
   * already been wrapped and do nothing.
   *
   * Only the container should ever actually close the servlet request stream.
   *
   * @param response
   *          The response to wrap.
   *          If this response corresponds to an original request or a retry.
   * @return A response object with an {@link OutputStream} that will ignore calls to close.
   */
  public static HttpServletResponse closeShield(HttpServletResponse response) {
    return new HttpServletResponseWrapper(response) {

      @Override
      public ServletOutputStream getOutputStream() throws IOException {

        return new ServletOutputStreamWrapper(super.getOutputStream()) {
          @Override
          public void close() {
            // even though we skip closes, we let local tests know not to close so that a full understanding can take
            // place
            assert Thread.currentThread().getStackTrace()[2].getClassName().matches(
                "org\\.apache\\.(?:solr|lucene).*") ? false
                    : true : CLOSE_STREAM_MSG;
            stream = ClosedServletOutputStream.CLOSED_SERVLET_OUTPUT_STREAM;
          }
        };
      }
      

      @Override
      public void sendError(int sc, String msg) throws IOException {
        response.setStatus(sc);
        response.getWriter().write(msg);
      }


      @Override
      public void sendError(int sc) throws IOException {
        sendError(sc, "Solr ran into an unexpected problem and doesn't seem to know more about it. There may be more information in the Solr logs.");
      }

    };

  }

  public void closeOnDestroy(boolean closeOnDestroy) {
    this.closeOnDestroy = closeOnDestroy;
  }
}
