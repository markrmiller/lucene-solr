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
package org.apache.solr.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

public class StopWatch {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  public final static ThreadLocal<StopWatch> STOP_WATCH = ThreadLocal.withInitial(StopWatch::new);
  private final boolean nonDebugActive;

  private long start;
  private String name;
  private volatile long time;

  public StopWatch(String name) {
    if (log.isDebugEnabled()) {
      this.name = "StopWatch-" + name;
    }
    this.nonDebugActive = false;
  }

  public StopWatch() {
    this.nonDebugActive = false;
  }

  public StopWatch(boolean nonDebugActive) {
    this.nonDebugActive = nonDebugActive;
  }

  public static StopWatch getStopWatch(String name) {
    StopWatch sw = STOP_WATCH.get();
    sw.start(name);
    return sw;
  }

  public void start(String name) {
    if (log.isDebugEnabled() || nonDebugActive) {
      this.name = name;
      start = System.nanoTime();
    }
  }

  public long getTime() {
    return time;
  }

  public void done() {
    if (log.isDebugEnabled() || nonDebugActive) {
      time = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
      log.debug("Time taken for {}={}ms", name, time);
    }
  }
}
