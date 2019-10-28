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
package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;

import java.util.concurrent.TimeUnit;

import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.patterns.SolrThreadSafe;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// this class may be accessed by multiple threads, but only one at a time
@SolrThreadSafe
public class ActionThrottle {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private volatile Long lastActionStartedAt;
  private volatile Long minMsBetweenActions;

  private final String name;
  private final TimeSource timeSource;

  public ActionThrottle(String name, long minMsBetweenActions) {
    this(name, minMsBetweenActions, TimeSource.NANO_TIME);
  }
  
  public ActionThrottle(String name, long minMsBetweenActions, TimeSource timeSource) {
    this.name = name;
    this.minMsBetweenActions = minMsBetweenActions;
    this.timeSource = timeSource;
  }

  public ActionThrottle(String name, long minMsBetweenActions, long lastActionStartedAt)  {
    this(name, minMsBetweenActions, lastActionStartedAt, TimeSource.NANO_TIME);
  }

  public ActionThrottle(String name, long minMsBetweenActions, long lastActionStartedAt, TimeSource timeSource)  {
    this.name = name;
    this.minMsBetweenActions = minMsBetweenActions;
    this.lastActionStartedAt = lastActionStartedAt;
    this.timeSource = timeSource;
  }

  public void reset() {
    if (log.isDebugEnabled()) {
      log.debug("reset() - start");
    }

    lastActionStartedAt = null;

    if (log.isDebugEnabled()) {
      log.debug("reset() - end");
    }
  }

  public void markAttemptingAction() {
    if (log.isDebugEnabled()) {
      log.debug("markAttemptingAction() - start");
    }

    lastActionStartedAt = timeSource.getTimeNs();

    if (log.isDebugEnabled()) {
      log.debug("markAttemptingAction() - end");
    }
  }
  
  public void minimumWaitBetweenActions() {
    if (log.isDebugEnabled()) {
      log.debug("minimumWaitBetweenActions() - start");
    }

    if (lastActionStartedAt == null) {
      if (log.isDebugEnabled()) {
        log.debug("minimumWaitBetweenActions() - end");
      }
      return;
    }
    long diff = timeSource.getTimeNs() - lastActionStartedAt;
    int diffMs = (int) TimeUnit.MILLISECONDS.convert(diff, TimeUnit.NANOSECONDS);
    long minNsBetweenActions = TimeUnit.NANOSECONDS.convert(minMsBetweenActions, TimeUnit.MILLISECONDS);
    if (log.isDebugEnabled()) log.debug("The last {} attempt started {}ms ago.", name, diffMs);
    int sleep = 0;
    
    if (diffMs > 0 && diff < minNsBetweenActions) {
      sleep = (int) TimeUnit.MILLISECONDS.convert(minNsBetweenActions - diff, TimeUnit.NANOSECONDS);
    } else if (diffMs == 0) {
      sleep = minMsBetweenActions.intValue();
    }
    
    if (sleep > 0) {
      log.info("Throttling {} attempts - waiting for {}ms", name, sleep);
      try {
        timeSource.sleep(sleep);
      } catch (InterruptedException e) {
        log.error("minimumWaitBetweenActions()", e);
        DW.propegateInterrupt(e);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("minimumWaitBetweenActions() - end");
    }
  }

  public Long getLastActionStartedAt() {
    return lastActionStartedAt;
  }
}
