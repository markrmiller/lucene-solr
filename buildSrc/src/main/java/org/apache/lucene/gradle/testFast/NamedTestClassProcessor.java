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

import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;

public class NamedTestClassProcessor implements TestClassProcessor {

  private final TestClassProcessor delegate;
  private final String name;

  public NamedTestClassProcessor(String name, TestClassProcessor delegate) {
    this.delegate = delegate;
    this.name = name;
  }
  
  @Override
  public void startProcessing(TestResultProcessor resultProcessor) {
    delegate.startProcessing(resultProcessor);
  }

  @Override
  public void processTestClass(TestClassRunInfo testClass) {
    delegate.processTestClass(testClass);
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public void stopNow() {
    delegate.stopNow();
  }

  public String getName() {
    return name;
  }

  @Override
  public String toString() {
    return "NamedTestClassProcessor [name=" + name + "]";
  }

}
