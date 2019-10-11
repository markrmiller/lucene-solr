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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseTimeTracker {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final Map<String,CloseTimeTracker> CLOSE_TIMES = new ConcurrentHashMap<>();
  
  private final long startTime;

  private volatile long doneTime;

  private volatile Object trackedObject;
  
  private final List<CloseTimeTracker> children = Collections.synchronizedList(new ArrayList<>());

  private final Class<? extends Object> clazz;

  private final StringBuilder label = new StringBuilder();
  
  private final int depth;
  
  public CloseTimeTracker(Object object, String label) {
    this(object, label, 1);
  }

  private CloseTimeTracker(Object object, String label, int i) {
    this.trackedObject = object;
    this.clazz = object == null ? null : object.getClass();
    this.startTime = System.nanoTime();
    this.label.append(label);
    this.depth = i;
    
    if (depth <= 1) {
      CLOSE_TIMES.put((object != null ? object.hashCode() : 0) + "_" + label.hashCode(), this);
    }
  }

  public void doneClose() {
    doneTime = System.nanoTime();
    //System.out.println("done close: " + trackedObject + " "  + label + " " + getElapsedNS());
  }
  
  public void doneClose(String label) {
   // if (theObject == null) return;
   // log.info(theObject instanceof String ? theObject.getClass().getName() : theObject.toString() +  " was closed");
    doneTime = System.nanoTime();
    
    //this.label.append(label);
    StringBuilder spacer = new StringBuilder();
    for (int i =0; i < depth; i++) {
      spacer.append(' ');
    }
    
    String extra = "";
    if (label.trim().length() != 0) {
      extra = label + "->";
    }
    
    this.label.insert(0, spacer.toString() + extra);
  }

  public long getElapsedNS() {
    return getElapsedNS(startTime, doneTime);
  }

  public CloseTimeTracker startSubClose(String label) {
    CloseTimeTracker subTracker = new CloseTimeTracker(null, label, depth+1);
    children.add(subTracker);
    return subTracker;
  }
  
  public CloseTimeTracker startSubClose(Object object) {
    CloseTimeTracker subTracker = new CloseTimeTracker(object, object.getClass().getName(), depth+1);
    children.add(subTracker);
    return subTracker;
  }
  
  public void printCloseTimes() {
    System.out.println("\n------\n" + getCloseTimes() + "\n------\n");
  }

  public String getCloseTimes() {
    StringBuilder sb = new StringBuilder();
//    if (trackedObject != null) {
//      if (trackedObject instanceof String) {
//        sb.append(label + trackedObject.toString() + " " + getElapsedMS() + "ms");
//      } else {
//        sb.append(label + trackedObject.getClass().getName() + " " + getElapsedMS() + "ms");
//      }
//    } else {
      sb.append(label + " " + getElapsedMS() + "ms");
//    }
     // sb.append("[\n");
    synchronized (children) {
     // sb.append(" children(" + children.size() + ")");
      for (CloseTimeTracker entry : children) {
        sb.append("\n");
        for (int i = 0; i < depth; i++) {
          sb.append(' ');
        }
        sb.append(entry.getCloseTimes());
      }
    }
    //sb.append("]\n");
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
    if (label != null) {
      return (children.size() > 0 ? ":" : "") + label + " " + getElapsedMS() + "ms";
    } else if (trackedObject != null) {
      if (trackedObject instanceof String) {
        return trackedObject.toString() + "-> " + getElapsedMS() + "ms";
      } else {
        return trackedObject.getClass().getSimpleName() + "--> " + getElapsedMS() + "ms";
      }
    }
    return "*InternalError*";
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
