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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseTimeTracker {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final Map<Integer,CloseTimeTracker> CLOSE_TIMES = new ConcurrentHashMap<>();
  
  private final long startTime;

  private volatile long doneTime;

//  private final Map<String,Long> startTimes = new ConcurrentHashMap<>();
//  private final Map<String,Long> endTimes = new ConcurrentHashMap<>();
  private volatile Object trackedObject;
  
  private final List<CloseTimeTracker> children = Collections.synchronizedList(new ArrayList<>());

  private final Class<? extends Object> clazz;

  private final StringBuilder label = new StringBuilder();

  public CloseTimeTracker(Object object) {
    log.info("Start tracking close for " + object.getClass().getName());
    this.clazz = object.getClass();
    
    CLOSE_TIMES.put(object.hashCode(), this);
    this.trackedObject = object;
    this.startTime = System.nanoTime();
  }
  
  private CloseTimeTracker(Object object, String label) {
    this.trackedObject = object;
    this.clazz = object.getClass();
    this.startTime = System.nanoTime();
    this.label.append(label);
  }

  private CloseTimeTracker(String label) {
    this.clazz = null;
    this.trackedObject = null;
    this.startTime = System.nanoTime();
    this.label.append(label);
  }

  public void doneClose() {
    doneTime = System.nanoTime();
    //System.out.println("done close: " + trackedObject + " "  + label + " " + getElapsedNS());
  }
  
  public void doneClose(Object theObject) {
    if (theObject == null) return;
    log.info(theObject instanceof String ? theObject.getClass().getName() : theObject.toString() +  " was closed");
    doneTime = System.nanoTime();
    trackedObject = theObject;
  }

  public long getElapsedNS() {
    return getElapsedNS(startTime, doneTime);
  }

  public CloseTimeTracker startSubClose(String label) {
    CloseTimeTracker subTracker = new CloseTimeTracker("  " + label);
    children.add(subTracker);
    return subTracker;
  }
  
  public CloseTimeTracker startSubClose(Object object) {
    CloseTimeTracker subTracker = new CloseTimeTracker(object, "  ");
    children.add(subTracker);
    return subTracker;
  }
  
  public void printCloseTimes() {
    System.out.println("\n------\n" + getCloseTimes() + "\n------\n");
  }

  public String getCloseTimes() {
    StringBuilder sb = new StringBuilder();
    if (trackedObject != null) {
      if (trackedObject instanceof String) {
        sb.append(label + trackedObject.toString() + " " + getElapsedMS() + "ms");
      } else {
        sb.append(label + trackedObject.getClass().getName() + " " + getElapsedMS() + "ms");
      }
    } else {
      sb.append(":" + label + " " + getElapsedMS() + "ms");
    }

    synchronized (children) {
      //sb.append("children:");
      for (CloseTimeTracker entry : children) {
        sb.append("\n  " + entry.getCloseTimes());
      }
    }

    return sb.toString();

    // synchronized (startTimes) {
    // synchronized (endTimes) {
    // for (String label : startTimes.keySet()) {
    // long startTime = startTimes.get(label);
    // long endTime = endTimes.get(label);
    // System.out.println(" -" + label + ": " + getElapsedMS(startTime, endTime) + "ms");
    // }
    // }
    // }
  }
  
  public String toString() {
    if (trackedObject != null) {
      if (trackedObject instanceof String) {
        return trackedObject.toString() + ": " + getElapsedMS() + "ms";
      } else {
        return trackedObject.getClass().getSimpleName() + ": " + getElapsedMS() + "ms";
      }
    } else {
      return ":" + label + " " + getElapsedMS() + "ms";
    }
  }

  private long getElapsedNS(long startTime, long doneTime) {
    return doneTime - startTime;
  }

  public Class<? extends Object> getClazz() {
    return clazz;
  }
  
  public long getElapsedMS() {
    long ms = getElapsedMS(startTime, doneTime);
    return ms < 0 ? 0 : ms;
  }

  public long getElapsedMS(long startTime, long doneTime) {
    return TimeUnit.MILLISECONDS.convert(doneTime - startTime, TimeUnit.NANOSECONDS);
  }


}
