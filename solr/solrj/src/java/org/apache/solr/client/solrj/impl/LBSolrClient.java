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

package org.apache.solr.client.solrj.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.IsUpdateRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.slf4j.MDC;

import static org.apache.solr.common.params.CommonParams.ADMIN_PATHS;

public abstract class LBSolrClient extends SolrClient {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // defaults
  private static final Set<Integer> RETRY_CODES = new HashSet<>(Arrays.asList(404, 403, 503, 500));
  private static final int CHECK_INTERVAL = 60 * 1000; //1 minute between checks
  private static final int NONSTANDARD_PING_LIMIT = 5;  // number of times we'll ping dead servers not in the server list

  // keys to the maps are currently of the form "http://localhost:8983/solr"
  // which should be equivalent to HttpSolrServer.getBaseURL()
  private final Map<String, ServerWrapper> aliveServers = new LinkedHashMap<>();
  // access to aliveServers should be synchronized on itself

  private final Map<String, ServerWrapper> zombieServers = new ConcurrentHashMap<>();

  // changes to aliveServers are reflected in this array, no need to synchronize
  private volatile ServerWrapper[] aliveServerList = new ServerWrapper[0];


  private volatile ScheduledExecutorService aliveCheckExecutor;

  private int interval = CHECK_INTERVAL;
  private final AtomicInteger counter = new AtomicInteger(-1);

  private static final SolrQuery solrQuery = new SolrQuery("*:*");
  protected volatile ResponseParser parser;
  protected volatile RequestWriter requestWriter;

  protected volatile Set<String> queryParams = Collections.unmodifiableSet(new HashSet<>());

  static {
    solrQuery.setRows(0);
    /**
     * Default sort (if we don't supply a sort) is by score and since
     * we request 0 rows any sorting and scoring is not necessary.
     * SolrQuery.DOCID schema-independently specifies a non-scoring sort.
     * <code>_docid_ asc</code> sort is efficient,
     * <code>_docid_ desc</code> sort is not, so choose ascending DOCID sort.
     */
    solrQuery.setSort(SolrQuery.DOCID, SolrQuery.ORDER.asc);
    // not a top-level request, we are interested only in the server being sent to i.e. it need not distribute our request to further servers
    solrQuery.setDistrib(false);
  }

  protected static class ServerWrapper {
    final String baseUrl;

    // "standard" servers are used by default.  They normally live in the alive list
    // and move to the zombie list when unavailable.  When they become available again,
    // they move back to the alive list.
    boolean standard = true;

    int failedPings = 0;

    ServerWrapper(String baseUrl) {
      this.baseUrl = baseUrl;
    }

    public String getBaseUrl() {
      return baseUrl;
    }

    @Override
    public String toString() {
      return baseUrl;
    }

    @Override
    public int hashCode() {
      return baseUrl.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof ServerWrapper)) return false;
      return baseUrl.equals(((ServerWrapper)obj).baseUrl);
    }
  }


  public static class Req {
    protected SolrRequest request;
    protected List<String> servers;
    protected int numDeadServersToTry;
    private final Integer numServersToTry;

    public Req(SolrRequest request, List<String> servers) {
      this(request, servers, null);
    }

    public Req(SolrRequest request, List<String> servers, Integer numServersToTry) {
      this.request = request;
      this.servers = servers;
      this.numDeadServersToTry = servers.size();
      this.numServersToTry = numServersToTry;
    }

    public SolrRequest getRequest() {
      return request;
    }
    public List<String> getServers() {
      return servers;
    }

    /** @return the number of dead servers to try if there are no live servers left */
    public int getNumDeadServersToTry() {
      return numDeadServersToTry;
    }

    /** @param numDeadServersToTry The number of dead servers to try if there are no live servers left.
     * Defaults to the number of servers in this request. */
    public void setNumDeadServersToTry(int numDeadServersToTry) {
      this.numDeadServersToTry = numDeadServersToTry;
    }

    public Integer getNumServersToTry() {
      return numServersToTry;
    }
  }

  public static class Rsp {
    protected String server;
    protected NamedList<Object> rsp;

    /** The response from the server */
    public NamedList<Object> getResponse() {
      return rsp;
    }

    /** The server that returned the response */
    public String getServer() {
      return server;
    }
  }

  public LBSolrClient(List<String> baseSolrUrls) {
    if (!baseSolrUrls.isEmpty()) {
      for (String s : baseSolrUrls) {
        ServerWrapper wrapper = createServerWrapper(s);
        aliveServers.put(wrapper.getBaseUrl(), wrapper);
      }
      updateAliveList();
    }
  }

  protected void updateAliveList() {
    if (log.isDebugEnabled()) {
      log.debug("updateAliveList() - start");
    }

    synchronized (aliveServers) {
      aliveServerList = aliveServers.values().toArray(new ServerWrapper[0]);
    }

    if (log.isDebugEnabled()) {
      log.debug("updateAliveList() - end");
    }
  }

  protected ServerWrapper createServerWrapper(String baseUrl) {
    if (log.isDebugEnabled()) {
      log.debug("createServerWrapper(String baseUrl={}) - start", baseUrl);
    }

    ServerWrapper returnServerWrapper = new ServerWrapper(baseUrl);
    if (log.isDebugEnabled()) {
      log.debug("createServerWrapper(String) - end");
    }
    return returnServerWrapper;
  }

  public Set<String> getQueryParams() {
    return queryParams;
  }

  /**
   * Expert Method.
   * @param queryParams set of param keys to only send via the query string
   */
  public void setQueryParams(Set<String> queryParams) {
    if (log.isDebugEnabled()) {
      log.debug("setQueryParams(Set<String> queryParams={}) - start", queryParams);
    }

    this.queryParams = Collections.unmodifiableSet(queryParams);

    if (log.isDebugEnabled()) {
      log.debug("setQueryParams(Set<String>) - end");
    }
  }
  public void addQueryParams(String queryOnlyParam) {
    if (log.isDebugEnabled()) {
      log.debug("addQueryParams(String queryOnlyParam={}) - start", queryOnlyParam);
    }

    HashSet<String> newSet = new HashSet<String>(this.queryParams);
    newSet.add(queryOnlyParam);

    this.queryParams = newSet;

    if (log.isDebugEnabled()) {
      log.debug("addQueryParams(String) - end");
    }
  }

  public static String normalize(String server) {
    if (log.isDebugEnabled()) {
      log.debug("normalize(String server={}) - start", server);
    }

    if (server.endsWith("/"))
      server = server.substring(0, server.length() - 1);

    if (log.isDebugEnabled()) {
      log.debug("normalize(String) - end");
    }
    return server;
  }


  /**
   * Tries to query a live server from the list provided in Req. Servers in the dead pool are skipped.
   * If a request fails due to an IOException, the server is moved to the dead pool for a certain period of
   * time, or until a test request on that server succeeds.
   *
   * Servers are queried in the exact order given (except servers currently in the dead pool are skipped).
   * If no live servers from the provided list remain to be tried, a number of previously skipped dead servers will be tried.
   * Req.getNumDeadServersToTry() controls how many dead servers will be tried.
   *
   * If no live servers are found a SolrServerException is thrown.
   *
   * @param req contains both the request as well as the list of servers to query
   *
   * @return the result of the request
   *
   * @throws IOException If there is a low-level I/O error.
   */
  public Rsp request(Req req) throws SolrServerException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("request(Req req={}) - start", req);
    }
    
    if (req.getServers().size() == 0) {
      throw new IllegalArgumentException("Must specify at least one server to make the request to");
    }

    Rsp rsp = new Rsp();
    Exception ex = null;
    boolean isNonRetryable = req.request instanceof IsUpdateRequest || ADMIN_PATHS.contains(req.request.getPath());
    List<ServerWrapper> skipped = null;

    final Integer numServersToTry = req.getNumServersToTry();
    int numServersTried = 0;

    boolean timeAllowedExceeded = false;
    long timeAllowedNano = getTimeAllowedInNanos(req.getRequest());
    long timeOutTime = System.nanoTime() + timeAllowedNano;
    for (String serverStr : req.getServers()) {
      if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      serverStr = normalize(serverStr);
      // if the server is currently a zombie, just skip to the next one
      ServerWrapper wrapper = zombieServers.get(serverStr);
      if (wrapper != null) {
        // System.out.println("ZOMBIE SERVER QUERIED: " + serverStr);
        final int numDeadServersToTry = req.getNumDeadServersToTry();
        if (numDeadServersToTry > 0) {
          if (skipped == null) {
            skipped = new ArrayList<>(numDeadServersToTry);
            skipped.add(wrapper);
          }
          else if (skipped.size() < numDeadServersToTry) {
            skipped.add(wrapper);
          }
        }
        continue;
      }
      try {
        MDC.put("LBSolrClient.url", serverStr);

        if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
          break;
        }

        ++numServersTried;
        ex = doRequest(serverStr, req, rsp, isNonRetryable, false);
        if (ex == null) {
          if (log.isDebugEnabled()) {
            log.debug("request(Req) - end");
          }
          return rsp; // SUCCESS
        }
      } finally {
        MDC.remove("LBSolrClient.url");
      }
    }

    // try the servers we previously skipped
    if (skipped != null) {
      for (ServerWrapper wrapper : skipped) {
        if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
          break;
        }

        if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
          break;
        }

        try {
          MDC.put("LBSolrClient.url", wrapper.getBaseUrl());
          ++numServersTried;
          ex = doRequest(wrapper.baseUrl, req, rsp, isNonRetryable, true);
          if (ex == null) {
            if (log.isDebugEnabled()) {
              log.debug("request(Req) - end");
            }
            return rsp; // SUCCESS
          }
        } finally {
          MDC.remove("LBSolrClient.url");
        }
      }
    }


    final String solrServerExceptionMessage;
    if (timeAllowedExceeded) {
      if (log.isDebugEnabled()) {
        log.debug("Time allowed to handle this request exceeded");
      }
      solrServerExceptionMessage = "Time allowed to handle this request exceeded";
    } else {
      if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
        solrServerExceptionMessage = "No live SolrServers available to handle this request:"
            + " numServersTried="+numServersTried
            + " numServersToTry="+numServersToTry.intValue();
      } else {
        solrServerExceptionMessage = "No live SolrServers available to handle this request";
      }
    }
    if (ex == null) {
      throw new SolrServerException(solrServerExceptionMessage);
    } else {
      log.error("Exception encountered", ex);
      throw new SolrServerException(solrServerExceptionMessage+":" + zombieServers.keySet(), ex);
    }
  }

  /**
   * @return time allowed in nanos, returns -1 if no time_allowed is specified.
   */
  private long getTimeAllowedInNanos(final SolrRequest req) {
    if (log.isDebugEnabled()) {
      log.debug("getTimeAllowedInNanos(SolrRequest req={}) - start", req);
    }

    SolrParams reqParams = req.getParams();
    long returnlong = reqParams == null ? -1 : TimeUnit.NANOSECONDS.convert(reqParams.getInt(CommonParams.TIME_ALLOWED, -1), TimeUnit.MILLISECONDS);
    if (log.isDebugEnabled()) {
      log.debug("getTimeAllowedInNanos(SolrRequest) - end return=" + returnlong);
    }
    return returnlong;
  }

  private boolean isTimeExceeded(long timeAllowedNano, long timeOutTime) {
    if (log.isDebugEnabled()) {
      log.debug("isTimeExceeded(long timeAllowedNano={}, long timeOutTime={}) - start", timeAllowedNano, timeOutTime);
    }

    boolean returnboolean = timeAllowedNano > 0 && System.nanoTime() > timeOutTime;
    if (log.isDebugEnabled()) {
      log.debug("isTimeExceeded(long, long) - end return={}", returnboolean);
    }
    return returnboolean;
  }

  protected Exception doRequest(String baseUrl, Req req, Rsp rsp, boolean isNonRetryable,
                                boolean isZombie) throws SolrServerException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("doRequest(String baseUrl={}, Req req={}, Rsp rsp={}, boolean isNonRetryable={}, boolean isZombie={}) - start", baseUrl, req, rsp, isNonRetryable, isZombie);
    }

    Exception ex = null;
    try {
      rsp.server = baseUrl;
      req.getRequest().setBasePath(baseUrl);
      rsp.rsp = getClient(baseUrl).request(req.getRequest(), (String) null);
      if (isZombie) {
        zombieServers.remove(baseUrl);
      }
    } catch (HttpSolrClient.RemoteExecutionException e){
      throw e;
    } catch(SolrException e) {
      log.error("doRequest(String=" + baseUrl + ", Req=" + req + ", Rsp=" + rsp + ", boolean=" + isNonRetryable + ", boolean=" + isZombie + ")", e);

      // we retry on 404 or 403 or 503 or 500
      // unless it's an update - then we only retry on connect exception
      if (!isNonRetryable && RETRY_CODES.contains(e.code())) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        // Server is alive but the request was likely malformed or invalid
        if (isZombie) {
          zombieServers.remove(baseUrl);
        }
        throw e;
      }
    } catch (SocketException e) {
      log.error("doRequest(String=" + baseUrl + ", Req=" + req + ", Rsp=" + rsp + ", boolean=" + isNonRetryable + ", boolean=" + isZombie + ")", e);

      if (!isNonRetryable || e instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (SocketTimeoutException e) {
      log.error("doRequest(String=" + baseUrl + ", Req=" + req + ", Rsp=" + rsp + ", boolean=" + isNonRetryable + ", boolean=" + isZombie + ")", e);

      if (!isNonRetryable) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (SolrServerException e) {
      log.error("doRequest(String=" + baseUrl + ", Req=" + req + ", Rsp=" + rsp + ", boolean=" + isNonRetryable + ", boolean=" + isZombie + ")", e);

      Throwable rootCause = e.getRootCause();
      if (!isNonRetryable && rootCause instanceof IOException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else if (isNonRetryable && rootCause instanceof ConnectException) {
        ex = (!isZombie) ? addZombie(baseUrl, e) : e;
      } else {
        throw e;
      }
    } catch (Exception e) {
      log.error("doRequest(String=" + baseUrl + ", Req=" + req + ", Rsp=" + rsp + ", boolean=" + isNonRetryable + ", boolean=" + isZombie + ")", e);

      SW.propegateInterrupt(e);
      throw new SolrServerException(e);
    }

    if (log.isDebugEnabled()) {
      log.debug("doRequest(String, Req, Rsp, boolean, boolean) - end");
    }
    return ex;
  }

  protected abstract SolrClient getClient(String baseUrl);

  private Exception addZombie(String serverStr, Exception e) {
    if (log.isDebugEnabled()) {
      log.debug("addZombie(String serverStr={}, Exception e={}) - start", serverStr, e);
    }

    ServerWrapper wrapper = createServerWrapper(serverStr);
    wrapper.standard = false;
    zombieServers.put(serverStr, wrapper);
    startAliveCheckExecutor();

    if (log.isDebugEnabled()) {
      log.debug("addZombie(String, Exception) - end");
    }
    return e;
  }

  /**
   * LBHttpSolrServer keeps pinging the dead servers at fixed interval to find if it is alive. Use this to set that
   * interval
   *
   * @param interval time in milliseconds
   */
  public void setAliveCheckInterval(int interval) {
    if (log.isDebugEnabled()) {
      log.debug("setAliveCheckInterval(int interval={}) - start", interval);
    }

    if (interval <= 0) {
      throw new IllegalArgumentException("Alive check interval must be " +
          "positive, specified value = " + interval);
    }
    this.interval = interval;

    if (log.isDebugEnabled()) {
      log.debug("setAliveCheckInterval(int) - end");
    }
  }

  private void startAliveCheckExecutor() {
    if (log.isDebugEnabled()) {
      log.debug("startAliveCheckExecutor() - start");
    }

    // double-checked locking, but it's OK because we don't *do* anything with aliveCheckExecutor
    // if it's not null.
    if (aliveCheckExecutor == null) {
      synchronized (this) {
        if (aliveCheckExecutor == null) {
          aliveCheckExecutor = Executors.newSingleThreadScheduledExecutor(
              new SolrjNamedThreadFactory("aliveCheckExecutor"));
          aliveCheckExecutor.scheduleAtFixedRate(
              getAliveCheckRunner(new WeakReference<>(this)),
              this.interval, this.interval, TimeUnit.MILLISECONDS);
        }
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("startAliveCheckExecutor() - end");
    }
  }

  private static Runnable getAliveCheckRunner(final WeakReference<LBSolrClient> lbRef) {
    if (log.isDebugEnabled()) {
      log.debug("getAliveCheckRunner(WeakReference<LBSolrClient> lbRef={}) - start", lbRef);
    }

    if (log.isDebugEnabled()) {
      log.debug("getAliveCheckRunner(WeakReference<LBSolrClient>) - end");
    }
    return () -> {
      LBSolrClient lb = lbRef.get();
      if (lb != null && lb.zombieServers != null) {
        for (Object zombieServer : lb.zombieServers.values()) {
          lb.checkAZombieServer((ServerWrapper)zombieServer);
        }
      }
    };
  }

  public ResponseParser getParser() {
    return parser;
  }

  /**
   * Changes the {@link ResponseParser} that will be used for the internal
   * SolrServer objects.
   *
   * @param parser Default Response Parser chosen to parse the response if the parser
   *               were not specified as part of the request.
   * @see org.apache.solr.client.solrj.SolrRequest#getResponseParser()
   */
  public void setParser(ResponseParser parser) {
    this.parser = parser;
  }

  /**
   * Changes the {@link RequestWriter} that will be used for the internal
   * SolrServer objects.
   *
   * @param requestWriter Default RequestWriter, used to encode requests sent to the server.
   */
  public void setRequestWriter(RequestWriter requestWriter) {
    this.requestWriter = requestWriter;
  }

  public RequestWriter getRequestWriter() {
    return requestWriter;
  }

  private void checkAZombieServer(ServerWrapper zombieServer) {
    if (log.isDebugEnabled()) {
      log.debug("checkAZombieServer(ServerWrapper zombieServer={}) - start", zombieServer);
    }

    try {
      QueryRequest queryRequest = new QueryRequest(solrQuery);
      queryRequest.setBasePath(zombieServer.baseUrl);
      QueryResponse resp = queryRequest.process(getClient(zombieServer.getBaseUrl()));
      if (resp.getStatus() == 0) {
        // server has come back up.
        // make sure to remove from zombies before adding to alive to avoid a race condition
        // where another thread could mark it down, move it back to zombie, and then we delete
        // from zombie and lose it forever.
        ServerWrapper wrapper = zombieServers.remove(zombieServer.getBaseUrl());
        if (wrapper != null) {
          wrapper.failedPings = 0;
          if (wrapper.standard) {
            addToAlive(wrapper);
          }
        } else {
          // something else already moved the server from zombie to alive
        }
      }
    } catch (Exception e) {
      log.error("checkAZombieServer(ServerWrapper=" + zombieServer + ")", e);

      SW.propegateInterrupt(e);
      //Expected. The server is still down.
      zombieServer.failedPings++;

      // If the server doesn't belong in the standard set belonging to this load balancer
      // then simply drop it after a certain number of failed pings.
      if (!zombieServer.standard && zombieServer.failedPings >= NONSTANDARD_PING_LIMIT) {
        zombieServers.remove(zombieServer.getBaseUrl());
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("checkAZombieServer(ServerWrapper) - end");
    }
  }

  private ServerWrapper removeFromAlive(String key) {
    if (log.isDebugEnabled()) {
      log.debug("removeFromAlive(String key={}) - start", key);
    }

    synchronized (aliveServers) {
      ServerWrapper wrapper = aliveServers.remove(key);
      if (wrapper != null)
        updateAliveList();

      if (log.isDebugEnabled()) {
        log.debug("removeFromAlive(String) - end");
      }
      return wrapper;
    }
  }


  private void addToAlive(ServerWrapper wrapper) {
    if (log.isDebugEnabled()) {
      log.debug("addToAlive(ServerWrapper wrapper={}) - start", wrapper);
    }

    synchronized (aliveServers) {
      ServerWrapper prev = aliveServers.put(wrapper.getBaseUrl(), wrapper);
      // TODO: warn if there was a previous entry?
      updateAliveList();
    }

    if (log.isDebugEnabled()) {
      log.debug("addToAlive(ServerWrapper) - end");
    }
  }

  public void addSolrServer(String server) throws MalformedURLException {
    if (log.isDebugEnabled()) {
      log.debug("addSolrServer(String server={}) - start", server);
    }

    addToAlive(createServerWrapper(server));

    if (log.isDebugEnabled()) {
      log.debug("addSolrServer(String) - end");
    }
  }

  public String removeSolrServer(String server) {
    if (log.isDebugEnabled()) {
      log.debug("removeSolrServer(String server={}) - start", server);
    }

    try {
      server = new URL(server).toExternalForm();
    } catch (MalformedURLException e) {
      log.error("removeSolrServer(String=" + server + ")", e);

      throw new RuntimeException(e);
    }
    if (server.endsWith("/")) {
      server = server.substring(0, server.length() - 1);
    }

    // there is a small race condition here - if the server is in the process of being moved between
    // lists, we could fail to remove it.
    removeFromAlive(server);
    zombieServers.remove(server);

    if (log.isDebugEnabled()) {
      log.debug("removeSolrServer(String) - end");
    }
    return null;
  }

  /**
   * Tries to query a live server. A SolrServerException is thrown if all servers are dead.
   * If the request failed due to IOException then the live server is moved to dead pool and the request is
   * retried on another live server.  After live servers are exhausted, any servers previously marked as dead
   * will be tried before failing the request.
   *
   * @param request the SolrRequest.
   *
   * @return response
   *
   * @throws IOException If there is a low-level I/O error.
   */
  @Override
  public NamedList<Object> request(final SolrRequest request, String collection)
      throws SolrServerException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("request(SolrRequest request={}, String collection={}) - start", request, collection);
    }

    NamedList<Object> returnNamedList = request(request, collection, null);
    if (log.isDebugEnabled()) {
      log.debug("request(SolrRequest, String) - end");
    }
    return returnNamedList;
  }

  public NamedList<Object> request(final SolrRequest request, String collection,
                                   final Integer numServersToTry) throws SolrServerException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("request(SolrRequest request={}, String collection={}, Integer numServersToTry={}) - start", request, collection, numServersToTry);
    }

    Exception ex = null;
    ServerWrapper[] serverList = aliveServerList;

    final int maxTries = (numServersToTry == null ? serverList.length : numServersToTry.intValue());
    int numServersTried = 0;
    Map<String,ServerWrapper> justFailed = null;

    boolean timeAllowedExceeded = false;
    long timeAllowedNano = getTimeAllowedInNanos(request);
    long timeOutTime = System.nanoTime() + timeAllowedNano;
    for (int attempts=0; attempts<maxTries; attempts++) {
      if (timeAllowedNano >= 0 && (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime))) {
        if (log.isDebugEnabled()) {
          log.debug("Time allowed exceeded, break");
        }
        break;
      }

      ServerWrapper wrapper = pickServer(serverList, request);
      try {
        ++numServersTried;
        request.setBasePath(wrapper.baseUrl);
        NamedList<Object> returnNamedList = getClient(wrapper.getBaseUrl()).request(request, collection);
        if (log.isDebugEnabled()) {
          log.debug("request(SolrRequest, String, Integer) - end");
        }
        return returnNamedList;
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        log.error("request(SolrRequest=" + request + ", String=" + collection + ", Integer=" + numServersToTry + ")", e);

        if (e.getRootCause() instanceof IOException) {
          ex = e;
          moveAliveToDead(wrapper);
          if (justFailed == null) justFailed = new HashMap<>();
          justFailed.put(wrapper.getBaseUrl(), wrapper);
        } else {
          throw e;
        }
      } catch (Exception e) {
        log.error("request(SolrRequest=" + request + ", String=" + collection + ", Integer=" + numServersToTry + ")", e);

        SW.propegateInterrupt(e);
        throw new SolrServerException(e);
      }
    }

    // try other standard servers that we didn't try just now
    for (ServerWrapper wrapper : zombieServers.values()) {
      if (timeAllowedExceeded = isTimeExceeded(timeAllowedNano, timeOutTime)) {
        break;
      }

      if (wrapper.standard==false || justFailed!=null && justFailed.containsKey(wrapper.getBaseUrl())) continue;
      try {
        ++numServersTried;
        request.setBasePath(wrapper.baseUrl);
        NamedList<Object> rsp = getClient(wrapper.baseUrl).request(request, collection);
        // remove from zombie list *before* adding to alive to avoid a race that could lose a server
        zombieServers.remove(wrapper.getBaseUrl());
        addToAlive(wrapper);

        if (log.isDebugEnabled()) {
          log.debug("request(SolrRequest, String, Integer) - end");
        }
        return rsp;
      } catch (SolrException e) {
        // Server is alive but the request was malformed or invalid
        throw e;
      } catch (SolrServerException e) {
        log.error("request(SolrRequest=" + request + ", String=" + collection + ", Integer=" + numServersToTry + ")", e);

        if (e.getRootCause() instanceof IOException) {
          ex = e;
          // still dead
        } else {
          throw e;
        }
      } catch (Exception e) {
        log.error("request(SolrRequest=" + request + ", String=" + collection + ", Integer=" + numServersToTry + ")", e);

        SW.propegateInterrupt(e);
        throw new SolrServerException(e);
      }
    }


    final String solrServerExceptionMessage;
    if (timeAllowedExceeded) {
      solrServerExceptionMessage = "Time allowed to handle this request exceeded";
    } else {
      if (numServersToTry != null && numServersTried > numServersToTry.intValue()) {
        solrServerExceptionMessage = "No live SolrServers available to handle this request:"
            + " numServersTried="+numServersTried
            + " numServersToTry="+numServersToTry.intValue();
      } else {
        solrServerExceptionMessage = "No live SolrServers available to handle this request";
      }
    }
    if (ex == null) {
      throw new SolrServerException(solrServerExceptionMessage);
    } else {
      throw new SolrServerException(solrServerExceptionMessage, ex);
    }
  }

  /**
   * Pick a server from list to execute request.
   * By default servers are picked in round-robin manner,
   * custom classes can override this method for more advance logic
   * @param aliveServerList list of currently alive servers
   * @param request the request will be sent to the picked server
   * @return the picked server
   */
  protected ServerWrapper pickServer(ServerWrapper[] aliveServerList, SolrRequest request) {
    if (log.isDebugEnabled()) {
      log.debug("pickServer(ServerWrapper[] aliveServerList={}, SolrRequest request={}) - start", aliveServerList, request);
    }

    int count = counter.incrementAndGet() & Integer.MAX_VALUE;
    ServerWrapper returnServerWrapper = aliveServerList[count % aliveServerList.length];
    if (log.isDebugEnabled()) {
      log.debug("pickServer(ServerWrapper[], SolrRequest) - end");
    }
    return returnServerWrapper;
  }

  private void moveAliveToDead(ServerWrapper wrapper) {
    if (log.isDebugEnabled()) {
      log.debug("moveAliveToDead(ServerWrapper wrapper={}) - start", wrapper);
    }

    wrapper = removeFromAlive(wrapper.getBaseUrl());
    if (wrapper == null)
      return;  // another thread already detected the failure and removed it
    zombieServers.put(wrapper.getBaseUrl(), wrapper);
    startAliveCheckExecutor();

    if (log.isDebugEnabled()) {
      log.debug("moveAliveToDead(ServerWrapper) - end");
    }
  }

  @Override
  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    synchronized (this) {
      if (aliveCheckExecutor != null) {
        aliveCheckExecutor.shutdownNow();
        ExecutorUtil.shutdownAndAwaitTermination(aliveCheckExecutor);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }
}
