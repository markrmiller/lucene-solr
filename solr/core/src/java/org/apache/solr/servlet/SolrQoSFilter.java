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
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.params.QoSParams;
import org.apache.solr.common.util.SysStats;
import org.eclipse.jetty.servlets.QoSFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// still working out the best way for this to work
public class SolrQoSFilter extends QoSFilter {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  static final String MAX_REQUESTS_INIT_PARAM = "maxRequests";
  static final String SUSPEND_INIT_PARAM = "suspendMs";
  static final int PROC_COUNT = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();

  protected volatile int _origMaxRequests;

  private static SysStats sysStats = ParWork.getSysStats();
  private volatile long lastUpdate;
  private volatile long highLoadAt;
  private volatile long offHighLoadAt;
  private volatile long lastCheckLoad;

  @Override
  public void init(FilterConfig filterConfig) {
    super.init(filterConfig);
    _origMaxRequests = Integer.getInteger("solr.concurrentRequests.max", 10000);
    super.setMaxRequests(_origMaxRequests);
    super.setSuspendMs(Integer.getInteger("solr.concurrentRequests.suspendms", 15000));
    super.setWaitMs(Integer.getInteger("solr.concurrentRequests.waitms", 5000));
  }

  @Override
  // MRM TODO: - this is all just test/prototype - we should extract an actual strategy for adjusting on load
  // allow the user to select and configure one
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    chain.doFilter(request, response);
    HttpServletRequest req = (HttpServletRequest) request;
    String source = req.getHeader(QoSParams.REQUEST_SOURCE);
    boolean imagePath = req.getPathInfo() != null && req.getPathInfo().startsWith("/img/");
    boolean externalRequest = !imagePath && (source == null || !source.equals(QoSParams.INTERNAL));
    if (log.isDebugEnabled()) log.debug("SolrQoSFilter {} {} {}", sysStats.getSystemLoad(), sysStats.getTotalUsage(), externalRequest);
    //log.info("SolrQoSFilter {} {} {}", sysStats.getSystemLoad(), sysStats.getTotalUsage(), externalRequest);

    if (externalRequest) {
      if (log.isDebugEnabled()) {
        log.debug("external request"); //MRM TODO:: remove when testing is done
      }

      if (highLoadAt == 0 && (offHighLoadAt == 0 || System.nanoTime() - offHighLoadAt > TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS))
        && (lastCheckLoad == 0 || System.nanoTime() - lastCheckLoad > TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS))) {
        lastCheckLoad = System.nanoTime();
        checkLoad();
      } else {
        if (highLoadAt > 0 && System.nanoTime() - highLoadAt > TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS)) {
          updateMaxRequests(_origMaxRequests, sysStats.getSystemLoad(), 0);
          highLoadAt = 0;
          offHighLoadAt = System.nanoTime();
        }
      }

      //chain.doFilter(req, response);
      super.doFilter(req, response, chain);

    } else {
      if (log.isDebugEnabled()) {
        log.debug("internal request, allow");
      }
      chain.doFilter(req, response);
    }
  }

  private void checkLoad() {
    double ourLoad = 0; //sysStats.getTotalUsage();
    int currentMaxRequests = getMaxRequests();
    if (log.isDebugEnabled()) {
      log.debug("Our individual load is {}", ourLoad);
    }
    double sLoad = sysStats.getSystemLoad();


//    if (lowStateLoad(sLoad, currentMaxRequests)) {
////      if (log.isDebugEnabled()) log.debug("set max concurrent requests to orig value {}", _origMaxRequests);
////      updateMaxRequests(_origMaxRequests, sLoad, ourLoad);
////    } else {
//      updateMaxRequests(Math.min(_origMaxRequests, _origMaxRequests), sLoad, ourLoad);
//    } else {

      if (hiLoadState(sLoad, currentMaxRequests)) {

        if (currentMaxRequests == _origMaxRequests) {
          updateMaxRequests(20, sLoad, ourLoad);
          offHighLoadAt = 0;
          highLoadAt = System.nanoTime();
        }
      }
  //  }
      // MRM TODO: - deal with no supported, use this as a fail safe with high and low watermark?
  }

  private boolean lowStateLoad(double sLoad, int currentMaxRequests) {
    return currentMaxRequests < _origMaxRequests && sLoad < .95d;
  }

  private boolean hiLoadState(double sLoad, int currentMaxRequests) {
    return sLoad > 1.1d;
  }

  private void updateMaxRequests(int max, double sLoad, double ourLoad) {
    int currentMax = getMaxRequests();
    if (max < currentMax) {

      log.warn("Set max request to {} sload={} ourload={}", max, sLoad, ourLoad);
      lastUpdate = System.currentTimeMillis();
      setMaxRequests(max);

    } else if (max > currentMax) {

      log.warn("Set max request to {} sload={} ourload={}", max, sLoad, ourLoad);
      lastUpdate = System.currentTimeMillis();
      setMaxRequests(max);
    }

  }

  protected int getPriority(ServletRequest request)
  {
    HttpServletRequest baseRequest = (HttpServletRequest)request;

    String pathInfo = baseRequest.getPathInfo();
    //log.info("pathInfo={}", pathInfo);

    if (pathInfo != null && pathInfo.equals("/admin/collections")) {
      return 9;
    } else if (pathInfo != null && pathInfo.contains("/update")) {
      return 7;
    } else if (pathInfo != null && pathInfo.contains("/select")) {
      return 8;
    } else if (pathInfo != null && pathInfo.contains("/cores")) {
      return 10;
    }

    return 0;
  }
}