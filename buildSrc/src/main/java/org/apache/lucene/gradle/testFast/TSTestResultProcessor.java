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

import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.tasks.testing.TestOutputEvent;

public class TSTestResultProcessor implements TestResultProcessor {



  private final TestResultProcessor delegate;

  public TSTestResultProcessor(TestResultProcessor delegate) {
    this.delegate = delegate;
  }


  /**
   * Notifies this processor that a test has started execution.
   */
  @Override
  public synchronized void started(TestDescriptorInternal test, TestStartEvent event) {
    delegate.started(test, event);
  }

  /**
   * Notifies this processor that a test has completed execution.
   */
  @Override
  public synchronized void completed(Object testId, TestCompleteEvent event) {
    delegate.completed(testId, event);
  }

  /**
   * Notifies this processor that a test has produced some output.
   */
  @Override
  public synchronized void output(Object testId, TestOutputEvent event) {
    delegate.output(testId, event);
  }

  /**
   * Notifies this processor that a failure has occurred in the given test.
   */
  @Override
  public synchronized void failure(Object testId, Throwable result) {
    delegate.failure(testId, result);
  }
}
