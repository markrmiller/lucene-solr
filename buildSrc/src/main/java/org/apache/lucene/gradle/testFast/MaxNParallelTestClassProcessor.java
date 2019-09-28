package org.apache.lucene.gradle.testFast;

import java.util.Arrays;
import java.util.List;

/*
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.Factory;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.concurrent.CompositeStoppable;
import org.gradle.internal.concurrent.Stoppable;

/**
 * Manages a set of parallel TestClassProcessors. Uses a simple round-robin algorithm to assign test classes to
 * processors.
 */
public class MaxNParallelTestClassProcessor implements TestClassProcessor {

  private static final Logger log = Logging.getLogger(MaxNParallelTestClassProcessor.class);

  private volatile boolean stoppedNow;

  private final TestComm testCommSlow;
  private TestComm testCommFast;

  private final JVMTestQueues slowJVMs;
  private final JVMTestQueues fastJVMs;

  public MaxNParallelTestClassProcessor(int maxProcessors, Factory<TestClassProcessor> factory,
      ActorFactory actorFactory, TestComm testCommSlow, TestComm testCommFast, JVMTestQueues slowJvms, JVMTestQueues fastJvms) {
    this.testCommSlow = testCommSlow;
    this.testCommFast = testCommFast;

    this.slowJVMs = slowJvms;
    this.fastJVMs = fastJvms;
  }

  @Override
  public void startProcessing(TestResultProcessor resultProcessor) {
    System.out.println("start processing maxn: " + resultProcessor.getClass().getName());
    // TSTestResultProcessor proc = new TSTestResultProcessor(resultProcessor);
    System.out.println("Start slow jvm processing ... ");
    slowJVMs.startProcessing(resultProcessor);

    if (fastJVMs != null) {
      System.out.println("Start fast jvm processing ... ");
      fastJVMs.startProcessing(resultProcessor);
    }
  }

  @Override
  public void processTestClass(TestClassRunInfo testClass) {
     System.out.println("Decide which JVM set to give " + testClass.getTestClassName() + " to");
    if (fastJVMs != null) {
      boolean isFastJvmTest = fastJVMs.getTestClasses().isStdTest(testClass.getTestClassName());

      // System.out.println("Found in fast:" + isFastJvmTest);

      int slowLowCost = slowJVMs.lowestJVMQueueCount();
      int fastLowCost = fastJVMs.lowestJVMQueueCount();

      if (isFastJvmTest || (testCommFast.isDoneSend() && fastLowCost < slowLowCost)) {
        if (!isFastJvmTest) {
          // System.out.println("Adding a slow JVM test to the fast JVMS ... " + testClass.getTestClassName());

          testCommFast.addTest(testCommSlow, testClass);
        }

        try {
          fastJVMs.addTest(testClass);
        } catch (Throwable e) {
          e.printStackTrace();
        }
        return;
      }
    }

    try {
      slowJVMs.addTest(testClass);
    } catch (Throwable e) {
      e.printStackTrace();
    }
  }

  @Override
  public void stop() {
    System.out.println("\nStopping " + this.getClass().getSimpleName() + " after completing submitted tests ...");

    // while ((slowJVMs.totalJVMQueueCount() != 0 && fastJVMs.totalJVMQueueCount() != 0) ||
    // (testCommSlow.getOutstanding() > 0 || testCommFast.getOutstanding() > 0)) {
    // if (slowJVMs.totalJVMQueueCount() == 0) {
    // testCommSlow.resumeTests(" Waiting for stop and slowJvms queues look empty. StartedNotCompleted: " +
    // testCommSlow.getTestClasses().getStartedNotCompleted());
    // }
    // if (fastJVMs.totalJVMQueueCount() == 0) {
    // testCommFast.resumeTests(" Waiting for stop and fastJvms queues look empty. StartedNotCompleted: " +
    // testCommFast.getTestClasses().getStartedNotCompleted());
    // }
    // try {
    // Thread.sleep(2000);
    // } catch (InterruptedException e) {
    // Thread.currentThread().interrupt();
    // }
    // }

    Stoppable[] stoppables;
    
    if (fastJVMs != null) {
      stoppables = new Stoppable[] {slowJVMs, fastJVMs};
    } else {
      stoppables = new Stoppable[] {slowJVMs};
    }
    
    List<Stoppable> jvms = Arrays.asList(stoppables);

    jvms.parallelStream().forEach((s) -> s.stop());

    testCommSlow.getTestClasses().printHints();
    if (testCommFast != null) testCommFast.getTestClasses().printHints();
  }

  @Override
  public void stopNow() {
    System.out.println("STOPPING MAXN PROC!");
    testCommSlow.getTestClasses().printHints();

    if (fastJVMs != null) testCommFast.getTestClasses().printHints();

    stoppedNow = true;
    if (fastJVMs != null) testCommFast.setStopNow(true);
    testCommSlow.setStopNow(true);
    if (fastJVMs != null) {
      synchronized (testCommFast.getTestFlowLock()) {
        testCommFast.getTestFlowLock().notifyAll();
      }
    }
    synchronized (testCommSlow.getTestFlowLock()) {
      testCommSlow.getTestFlowLock().notifyAll();
    }
    if (fastJVMs != null) fastJVMs.stopNow();
    slowJVMs.stopNow();
  }
}
