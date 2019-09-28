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
package org.apache.lucene.gradle.testFast;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

public class TestComm {
  private static final Logger log = Logging.getLogger(TestComm.class);

  public final AtomicInteger completed = new AtomicInteger();
  
  public final AtomicLong startTime = new AtomicLong();

  private final Object testFlowLock = new Object();

  private final Object doneSendLock = new Object();

  private final TestClasses testClasses;

  private volatile boolean sendTests = true;

  private volatile boolean stopNow = false;

  private volatile boolean doneSend = false;

  private final int maxJvms;

  private final String name;

  private final AtomicInteger maxOutstanding = new AtomicInteger();

  private final AtomicInteger lowCost = new AtomicInteger();

  private final boolean raiseLowCost;

  private final double pctStdTests;
  
  private volatile static long lastLogTime;

  public TestComm(String name, int maxJvms, int maxOutstanding, int lowCost, boolean raiseLowCost, double pctStdTests) {
    this.maxJvms = maxJvms;
    this.maxOutstanding.set(maxOutstanding);
    this.name = name;
    this.lowCost.set(lowCost);
    this.raiseLowCost = raiseLowCost;
    this.testClasses = new TestClasses(name + " Tests");
    this.pctStdTests = pctStdTests;
  }

  public TestComm(String name, int maxJvms, int maxOutstanding, int lowCost, boolean raiseLowCost) {
    this(name, maxJvms, maxOutstanding, lowCost, raiseLowCost, 0);
  }

  public Object getTestFlowLock() {
    return testFlowLock;
  }

  public Object getDoneSendLock() {
    return doneSendLock;
  }

  public TestClasses getTestClasses() {
    assert testClasses != null;
    return testClasses;
  }

  public void resumeTests(String reason) {
    System.out.println("\n" + getName() + " resume tests " + reason);
    synchronized (getTestFlowLock()) {
      sendTests = true;
      getTestFlowLock().notifyAll();
    }
  }

  public void pauseTests(String reason) {
    System.out.println("\n" + getName() + " pause tests " + reason);
    sendTests = false;
  }

  public void setStopNow(boolean stopNow) {
    this.stopNow = stopNow;
  }

  public boolean isSendTests() {
    return sendTests;
  }

  public boolean isStopNow() {
    return stopNow;
  }

  public boolean isDoneSend() {
    return doneSend;
  }

  public void doneSend() {
    this.doneSend = true;
    synchronized (this.doneSendLock) {
      this.doneSendLock.notifyAll();
    }
  }

  public int getMaxOutstanding() {
    return maxOutstanding.get();
  }

  public String getName() {
    return name;
  }

  public int getOutstanding() {
    return getTestClasses().getOutstanding();
  }

  public int getStartedOutstanding() {
    return getTestClasses().getStartedOutstanding();
  }

  public void addTest(TestComm testCommFast, TestClassRunInfo testClass) {

    this.testClasses.addTest(testCommFast.testClasses, testClass);
  }

  public void raiseCosts(int lowCostMult, int outstandingMult) {
    if (raiseLowCost) {
      int newLowCost = lowCost.get() * lowCostMult;
      lowCost.set(newLowCost);
      int newMaxOutstanding = maxOutstanding.get() * outstandingMult;
      maxOutstanding.set(newMaxOutstanding);
    }
  }

  public int getLowCost() {
    return lowCost.get();
  }

  public int getMaxJvms() {
    return maxJvms;
  }

  public double getPctStdTests() {
    return pctStdTests;
  }

  public void setStartTime(long startTime) {
    this.startTime.set(startTime);
    this.testClasses.setStartTime(startTime);
  }
  
  public long sSinceStart() {
    return TimeUnit.SECONDS.convert(System.nanoTime() - startTime.get(), TimeUnit.NANOSECONDS);
  }
  
  public void log(String string) {
    if (System.nanoTime() - lastLogTime < 10000000000l) return;
    
    
    lastLogTime = System.nanoTime();
    System.out.println(sSinceStart() + "  " + string);
  }
  
  public static void log(String string, long sinceStart) {
    if (System.nanoTime() - lastLogTime < 10000000000l) return;
    
    
    lastLogTime = System.nanoTime();
    System.out.println(sinceStart + "  " + string);
  }

}
