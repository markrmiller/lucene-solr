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

import java.util.concurrent.atomic.AtomicInteger;

import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.tasks.testing.TestOutputEvent;

public class TestResultProcessorWrapper implements TestResultProcessor {

  public final AtomicInteger started = new AtomicInteger();
  public final AtomicInteger completed = new AtomicInteger();
  public final AtomicInteger failure = new AtomicInteger();
  private final TestResultProcessor processor;
  private final TestComm testCommSlow;
  private final TestComm testCommFast;
  private final JVMTestQueues slowJvms;
  private final JVMTestQueues fastJvms;

  public TestResultProcessorWrapper(TestResultProcessor processor, TestComm testCommSlow, TestComm testCommFast, JVMTestQueues slowJvms, JVMTestQueues fastJvms) {
    this.processor = processor;
    this.testCommSlow = testCommSlow;
    this.testCommFast = testCommFast;
    this.slowJvms = slowJvms;
    this.fastJvms = fastJvms;

  }

  /**
   * Notifies this processor that a test has started execution.
   */
  @Override
  public void started(TestDescriptorInternal test, TestStartEvent event) {
    try {
      started.getAndIncrement();

      testCommSlow.getTestClasses().startedEvent(test, test.getId(), System.nanoTime());
      if (testCommFast != null) testCommFast.getTestClasses().startedEvent(test, test.getId(), System.nanoTime());
    } finally {
      processor.started(test, event);
    }
  }

  /**
   * Notifies this processor that a test has completed execution.
   */
  @Override
  public void completed(Object testId, TestCompleteEvent event) {
    // if (FastParallelTestExecuter.DEBUG) System.out.println("completed " + " id:" + testId + " " +
    // event.getResultType());
    try {
      completed.getAndIncrement();

      testCommSlow.getTestClasses().completed(testId, null);
      if (testCommFast != null) testCommFast.getTestClasses().completed(testId, null);
      
      slowJvms.testDone(testId);
      if (fastJvms != null) fastJvms.testDone(testId);

    } finally {
      processor.completed(testId, event);
    }

  }

  /**
   * Notifies this processor that a test has produced some output.
   */
  @Override
  public void output(Object testId, TestOutputEvent event) {
    processor.output(testId, event);
  }

  /**
   * Notifies this processor that a failure has occurred in the given test.
   */
  @Override
  public void failure(Object testId, Throwable result) {
    failure.getAndIncrement();
    processor.failure(testId, result);

  }

}
