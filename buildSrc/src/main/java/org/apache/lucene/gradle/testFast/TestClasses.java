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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.gradle.api.internal.tasks.testing.DefaultTestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.tasks.testing.TestResult;

public class TestClasses {

  private List<TestClassRunInfo> slowTests = Collections.synchronizedList(new ArrayList<>());
  private List<TestClassRunInfo> slowestTests = Collections.synchronizedList(new ArrayList<>());
  List<TestClassRunInfo> stdTests = Collections.synchronizedList(new ArrayList<>());
  private List<TestClassRunInfo> nightlyTests = Collections.synchronizedList(new ArrayList<>());
  private List<TestClassRunInfo> ignoredTests = Collections.synchronizedList(new ArrayList<>());

  private Map<Object,Long> startTimes = new ConcurrentHashMap<>();
  private Map<String,Long> submittedTimes = new ConcurrentHashMap<>();
  private Map<Object,String> idToTest = new ConcurrentHashMap<>();
  private Map<String,Object> testToId = new ConcurrentHashMap<>();
  private Map<Object,TestDescriptorInternal> startedNotCompleted = new ConcurrentHashMap<>();

  public final AtomicInteger completed = new AtomicInteger();
  public final AtomicInteger submitted = new AtomicInteger();
  public final AtomicInteger started = new AtomicInteger();

  private final StringBuilder hints = new StringBuilder();
  private String name;

  public final AtomicLong startTime = new AtomicLong();

  public TestClasses(String name) {
    this.name = name;
  }

  public void startedEvent(TestDescriptorInternal testDesc, Object id, long startTime) {
    if (!testDesc.getName().startsWith("Gradle Test Executor") && testDesc.getOwnerBuildOperationId() == null) {
      // System.out.println("!!!!!!!!!!!!!! PUT: " + testDesc.getName() + " " + testDesc.getClassDisplayName() + " "+
      // testDesc.getOwnerBuildOperationId() + " " + testDesc.getParent());
      startedNotCompleted.put(id, testDesc);
    }
    if (testDesc.isComposite() && testDesc.getClassName() != null && testDesc.getId() != null) {
      String test = testDesc.getClassName();
      if (!ourTest(test)) {
        return;
      }
      started.incrementAndGet();
      startTimes.put(id, startTime);
      idToTest.put(id, test);
      testToId.put(test, id);
    }
  }

  public boolean ourTest(String name) {
    return isStdTest(name) || isNightlytTest(name) || isSlowestTest(name) || isSlowTest(name);
  }

  // public void completedEvent(Object testId, TestCompleteEvent event) {
  // startedNotCompleted.remove(testId);
  //
  // String testName = idToTest.get(testId);
  // if (testName != null) {
  // completed.getAndIncrement();
  // }
  // }

  public List<TestClassRunInfo> getSlowTests() {
    List<TestClassRunInfo> slowTestsCopy = new ArrayList<>();
    synchronized (slowTests) {
      slowTestsCopy.addAll(slowTests);
    }

    return slowTestsCopy;
  }

  public void setSlowTests(List<TestClassRunInfo> slowTests) {
    this.slowTests = slowTests;
  }

  public List<TestClassRunInfo> getSlowestTests() {
    List<TestClassRunInfo> slowestTestsCopy = new ArrayList<>();
    synchronized (slowestTests) {
      slowestTestsCopy.addAll(slowestTests);
    }
    return slowestTestsCopy;
  }

  public Map<Object,TestDescriptorInternal> getStartedNotCompleted() {
    return startedNotCompleted;
  }

  public void setSlowestTests(List<TestClassRunInfo> slowestTests) {
    this.slowestTests = slowestTests;
  }

  public void clearStdTests() {
    synchronized (stdTests) {
      stdTests.clear();
    }
  }

  public List<TestClassRunInfo> getStdTests() {
    List<TestClassRunInfo> stdTestsCopy = new ArrayList<>();
    synchronized (stdTests) {
      stdTestsCopy.addAll(stdTests);
    }
    return stdTestsCopy;
  }

  public List<TestClassRunInfo> getNightlyTests() {
    List<TestClassRunInfo> nightlyTestsCopy = new ArrayList<>();
    synchronized (nightlyTests) {
      nightlyTestsCopy.addAll(nightlyTests);
    }
    return nightlyTestsCopy;
  }

  public List<TestClassRunInfo> getIgnoredTests() {
    List<TestClassRunInfo> ignoredTestsCopy = new ArrayList<>();
    synchronized (ignoredTests) {
      ignoredTestsCopy.addAll(ignoredTests);
    }
    return ignoredTestsCopy;
  }

  public void setStdTests(List<TestClassRunInfo> stdTests) {
    this.stdTests = stdTests;
  }

  public Map<Object,Long> getStartTimes() {
    return startTimes;
  }

  public void setStartTimes(Map<Object,Long> startTimes) {
    this.startTimes = startTimes;
  }

  public Map<Object,String> getIdToTest() {
    return idToTest;
  }

  public void setIdToTest(Map<Object,String> idToTest) {
    this.idToTest = idToTest;
  }

  public Map<String,Object> getTestToId() {
    return testToId;
  }

  public void setTestToId(Map<String,Object> testToId) {
    this.testToId = testToId;
  }

  public void completed(Object id, TestResult result) {
    startedNotCompleted.remove(id);
    // Object id = testToId.get(testId);

    if (id == null) {
      System.out.println("could not find id for:" + id);
      return;
    }

    String suiteName = this.idToTest.get(id);

    if (suiteName == null) {
      // System.out.println("Could not find suite name for: " + id);
      return;
    }

    completed.incrementAndGet();
    Long startTime = startTimes.get(id);

    if (startTime == null) {
      throw new IllegalStateException("Could not find start time for: " + suiteName);
    }

    Long submittedTime = submittedTimes.get(suiteName);

    if (submittedTime == null) {
      throw new IllegalStateException("Could not find submitted time for: " + suiteName);
    }

    long totalTime = TimeUnit.SECONDS.convert(System.nanoTime() - startTime, TimeUnit.NANOSECONDS);
    StringBuilder sb = new StringBuilder();
    String correction = getCorrectAnnotation(isSlowTest(suiteName), isSlowestTest(suiteName), isNightlytTest(suiteName),
        isIgnoredTest(suiteName), totalTime);
    // if (FastParallelTestExecuter.DEBUG)
    sb.append("\nAfter: " + suiteName + " " + totalTime + "s");
    sb.append(" -> " + getTestDetails(suiteName));
    long timeSinceSubmit = TimeUnit.SECONDS.convert(submittedTime - this.startTime.get(), TimeUnit.NANOSECONDS);
    sb.append(
        " TIQ: " + TimeUnit.SECONDS.convert(startTime - submittedTime, TimeUnit.NANOSECONDS) + " started:" + timeSinceSubmit +"s\n\n");
    TestComm.log(sb.toString(), this.startTime.get());

    if (correction.length() > 0) {
      // System.out.println(sb.toString());
      hints.append("\n" + suiteName + " " + totalTime + "s" + " " + (isSlowTest(suiteName) ? "@Slow " : "")
          + (isSlowestTest(suiteName) ? "@Slowest " : "") + (isNightlytTest(suiteName) ? "@Nightly " : ""));
      hints.append(correction + "\n");
    }

  }

  public String getTestDetails(String suiteName) {
    if (suiteName == null) return "";
    return (isNightlytTest(suiteName) ? "@Nightly" : "") + (isStdTest(suiteName) ? "@Std" : "")
        + (isSlowTest(suiteName) ? "@Slow" : "") + (isSlowestTest(suiteName) ? "@Slowest" : "");
  }

  private String getCorrectAnnotation(boolean slow, boolean slowest, boolean nightly, boolean ignored, long totalTime) {
    // StringBuilder sb = new StringBuilder();
    if ((slow || slowest) && (totalTime < 8) && !(nightly || ignored)) {
      return " Found very fast test annotated as slow!";
    }

    if (slowest && totalTime < 30) {
      return " Found a test annotated slowest that just looks slow.";
    }

    if (slow && totalTime > 40) {
      return " Found a test annotated slow that just looks slowest.";
    }

    if (!slow && !slowest && totalTime > 10) {
      return " !!! Found a slow test not annotated !!!";
    }

    return "";
  }

  public void addStdTest(TestClassRunInfo testClass) {
    //System.out.println(getName() + " Add Std test:" + testClass.getTestClassName());
    stdTests.add(testClass);
  }

  public void addSlowestTest(TestClassRunInfo testClass) {
    //System.out.println(getName() + " Add Slowest test:" + testClass.getTestClassName());
    slowestTests.add(testClass);
  }

  public void addSlowTest(TestClassRunInfo testClass) {
    //System.out.println(getName() + " Add Slow test:" + testClass.getTestClassName());
    slowTests.add(testClass);
  }

  public void addNightlyTest(TestClassRunInfo testClass) {
    //System.out.println(getName() + " Add Nightly test:" + testClass.getTestClassName());
    nightlyTests.add(testClass);
  }

  public void addIgnoredTest(TestClassRunInfo testClass) {
    ignoredTests.add(testClass);
  }

  public void printCatSizes() {
    // if (FastParallelTestExecuter.DEBUG) {
    System.out.println(getName() + ": Slowest tests:" + slowestTests.size());
    System.out.println(getName() + ": Slow tests:" + slowTests.size());
    System.out.println(getName() + ": Std tests:" + stdTests.size());
    // }
  }

  private String getName() {
    return this.name;
  }

  public void shuffle() {
    synchronized (slowestTests) {
      Collections.shuffle(slowestTests);
    }
    synchronized (slowestTests) {
      Collections.shuffle(slowTests);
    }
    synchronized (slowestTests) {
      Collections.shuffle(stdTests);
    }
    synchronized (slowestTests) {
      Collections.shuffle(nightlyTests);
    }
    synchronized (slowestTests) {
      Collections.shuffle(ignoredTests);
    }
  }

  public int getCompleted() {
    return completed.get();
  }

  public boolean isSlowestTest(String test) {
    return slowestTests.contains(new DefaultTestClassRunInfo(test));
  }

  public boolean isNightlytTest(String test) {
    return nightlyTests.contains(new DefaultTestClassRunInfo(test));
  }

  public boolean isIgnoredTest(String test) {
    return ignoredTests.contains(new DefaultTestClassRunInfo(test));
  }

  public boolean isSlowTest(String test) {
    return slowTests.contains(new DefaultTestClassRunInfo(test));
  }

  public boolean isStdTest(String test) {
    return stdTests.contains(new DefaultTestClassRunInfo(test));
  }

  public void submittedEvent(String test) {
    try {
      submitted.incrementAndGet();
      TestComm.log("\nSubmit Test ( " + test + getTestDetails(test) + " )\n", startTime.get());

      submittedTimes.put(test, System.nanoTime());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public int getOutStanding() {
    return 0;
  }

  public int getSubmitted() {
    return submitted.get();
  }

  public int getOutstanding() {
    return submitted.get() - completed.get();
  }

  public int getStartedOutstanding() {
    return submitted.get() - started.get();
  }

  public void printHints() {
    System.out.println(hints.toString());
  }

  public void addStdTestsToList(List<TestClassRunInfo> stdTests, double pctOfTests) {
    synchronized (this.stdTests) {
      synchronized (stdTests) {
        stdTests.addAll(getQueueHead(this.stdTests, (int) Math.round(this.stdTests.size() * pctOfTests)));
      }
    }
  }

  private List<TestClassRunInfo> getQueueHead(List<TestClassRunInfo> tests, int size) {
    if (!(size > 10)) {
      return Collections.emptyList();
    }
    List<TestClassRunInfo> head = new ArrayList<>();

    synchronized (tests) {
      if (tests.size() >= size) {
        head.addAll(tests.subList(0, size - 1));
      } else {
        head.addAll(tests);
      }
    }

    tests.removeAll(head);

    return head;
  }

  public void addTest(TestClasses testClassesOther, TestClassRunInfo testClass) {
    if (testClassesOther.isStdTest(testClass.getTestClassName())) {
      this.stdTests.add(testClass);
    }
    if (testClassesOther.isSlowestTest(testClass.getTestClassName())) {
      this.slowestTests.add(testClass);
    }
    if (testClassesOther.isSlowTest(testClass.getTestClassName())) {
      this.slowTests.add(testClass);
    }
    if (testClassesOther.isNightlytTest(testClass.getTestClassName())) {
      this.nightlyTests.add(testClass);
    }
    if (testClassesOther.isIgnoredTest(testClass.getTestClassName())) {
      this.ignoredTests.add(testClass);
    }

    Long sub = testClassesOther.submittedTimes.get(testClass.getTestClassName());
    submittedTimes.put(testClass.getTestClassName(), sub);

    Object id = testClassesOther.testToId.get(testClass.getTestClassName());
    if (id == null) {
      return;
    }
    testToId.put(testClass.getTestClassName(), id);

    idToTest.put(id, testClass.getTestClassName());

    Long s = testClassesOther.startTimes.get(id);
    if (s != null) {
      startTimes.put(id, s);
      started.incrementAndGet();
      testClassesOther.started.decrementAndGet();
    }

    submitted.incrementAndGet();
    testClassesOther.submitted.decrementAndGet();
  }

  public void setStartTime(long startTime) {
    this.startTime.set(startTime);
  }

}
