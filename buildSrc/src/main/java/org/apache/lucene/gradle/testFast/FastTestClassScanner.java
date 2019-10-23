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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.gradle.api.file.EmptyFileVisitor;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.FileVisitDetails;
import org.gradle.api.internal.tasks.testing.DefaultTestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestClassRunInfo;
import org.gradle.api.internal.tasks.testing.TestCompleteEvent;
import org.gradle.api.internal.tasks.testing.TestDescriptorInternal;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.TestStartEvent;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.internal.tasks.testing.filter.TestSelectionMatcher;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestOutputEvent;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.internal.IoActions;
import org.objectweb.asm.ClassReader;

/**
 * The default test class scanner. Depending on the availability of a test framework detector, a detection or filename
 * scan is performed to find test classes.
 */
public class FastTestClassScanner implements Runnable {
  private static final Logger log = Logging.getLogger(FastTestClassScanner.class);

  private static final Pattern ANONYMOUS_CLASS_NAME = Pattern.compile(".*\\$\\d+");
  private final FileTree candidateClassFiles;
  private final TestClassProcessor testClassProcessor;
  private final TestSelectionMatcher testClassSelectionMatcher;

  private final TestComm testCommSlow;
  private final TestComm testCommFast;

  private int maxJvms;

  public FastTestClassScanner(FileTree candidateClassFiles,
      TestClassProcessor testClassProcessor, DefaultTestFilter testFilter, TestComm testCommSlow, TestComm testCommFast,
      int maxJvms) {
    this.candidateClassFiles = candidateClassFiles;
    this.testClassProcessor = testClassProcessor;
    this.testClassSelectionMatcher = new TestSelectionMatcher(
        testFilter.getIncludePatterns(), testFilter.getExcludePatterns(),
        testFilter.getCommandLineIncludePatterns());
    this.testCommSlow = testCommSlow;
    this.testCommFast = testCommFast;
    this.maxJvms = maxJvms;
    System.out.println("Test Include:" + testFilter.getIncludePatterns());
    System.out.println("Test Include:" + testFilter.getCommandLineIncludePatterns());
  }

  @Override
  public void run() {
    filenameScan();

    // while (testClasses.getOutstanding() > 0) {
    // System.out.println("Outstanding tests:" + testClasses.getOutstanding());
    // try {
    // Thread.currentThread().sleep(5000);
    // } catch (InterruptedException e) {
    //
    // }
    // }

  }

  private void filenameScan() {
    int numTests = candidateClassFiles.getFiles().size();
    if (FastParallelTestExecuter.DEBUG) System.out.println("file scan: " + numTests);

    candidateClassFiles.visit(new ClassFileVisitor() {
      @Override
      public void visitClassFile(FileVisitDetails fileDetails) {
        String className = getClassName(fileDetails);
        try {

          int index = className.lastIndexOf('.');
          String simpleName = className.substring(index + 1, className.length());

          if ((simpleName.startsWith("Test") || simpleName.endsWith("Test")) && !simpleName.contains("$")) {
            Result result = readClassFile(fileDetails.getFile(), getClassName(fileDetails));

            TestClassRunInfo testClass = new DefaultTestClassRunInfo(getClassName(fileDetails));

            if (!Boolean.getBoolean("tests.nightly") && result.hasNightlyAnnotation) {
              testCommSlow.getTestClasses().addNightlyTest(testClass);
            } else if (result.hasIgnoreAnnotation) {
              testCommSlow.getTestClasses().addIgnoredTest(testClass);
            } else if (result.hasSlowestAnnotation) {
              testCommSlow.getTestClasses().addSlowestTest(testClass);
            } else if (result.hasSlowAnnotation) {
              testCommSlow.getTestClasses().addSlowTest(testClass);
            } else {
              testCommSlow.getTestClasses().addStdTest(testClass);
            }
          }
        } catch (Exception e) {
          log.error("Exception visiting class file", e);
        }
      }
    });

    // testCommFast.getTestClasses().getStdTests().addAll(c).

    if (testCommFast != null && testCommFast.getMaxJvms() > 0) {
      testCommSlow.getTestClasses().addStdTestsToList(testCommFast.getTestClasses().stdTests,
          testCommFast.getPctStdTests());
      System.out.println("Fast JVMs taking std tests: " + testCommFast.getTestClasses().getStdTests());
    }

    Thread thread = new Thread() {
      public void run() {
        boolean shouldStop = processTests(testCommSlow);
        testCommSlow.doneSend();
      }
    };

    // Thread thread2 = new Thread() {
    // public void run() {
    // boolean shouldStop = processTests(testCommFast);
    // testCommFast.doneSend();
    // }
    // };

    long startTime = System.nanoTime();
    testCommSlow.setStartTime(startTime);
    if (testCommFast != null) testCommFast.setStartTime(startTime);

    boolean shouldStop = processTests(testCommSlow);
    testCommSlow.doneSend();

    // System.out.println("Starting submit thread(s).");
    // thread.start();
    // thread2.start();
    //
    // try {
    //
    // thread.join(3000);
    // // thread2.join(3000);
    //
    // } catch (InterruptedException e) {
    // throw new RuntimeException(e);
    // }

    while (true) {
      if (testCommFast != null && testCommFast.getTestClasses().getStartedNotCompleted().size() > 0
          || testCommSlow.getTestClasses().getStartedNotCompleted().size() > 0) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        // System.out.println("Fast: " + testCommFast.getTestClasses().getStartedNotCompleted());
        // System.out.println("Slow: " + testCommSlow.getTestClasses().getStartedNotCompleted());
      } else {
        break;
      }

    }

    // if (shouldStop) return;

  }

  private boolean processTests(TestComm testComm) {
    testComm.getTestClasses().printCatSizes();

    testComm.getTestClasses().shuffle();

    System.out.println("\nSend slowest tests ...\n");

    boolean shouldStop = runTests(testComm, testComm.getTestClasses().getSlowestTests());

    if (shouldStop) return true;

    System.out.println("\nSend slow tests ...\n");

    // outstanding, cost multiplier
    testComm.raiseCosts(2, 1);

    shouldStop = runTests(testComm, testComm.getTestClasses().getSlowTests());

    if (shouldStop) return true;

    System.out.println("\nSend std tests ...\n");

    testComm.raiseCosts(1, 2);

    shouldStop = runTests(testComm, testComm.getTestClasses().getStdTests());

    if (shouldStop) return true;

    // System.out.println("\nSend nightly tests ...\n");
    //
    // shouldStop = runTests(testComm, testComm.getTestClasses().getNightlyTests());
    //
    // if (shouldStop) return true;
    //
    // System.out.println("\nSend ignore tests ...\n");
    //
    // shouldStop = runTests(testComm, testComm.getTestClasses().getIgnoredTests());
    //
    // if (shouldStop) return true;

    return false;
  }

  private final Lock lock = new ReentrantLock(false);

  private boolean runTests(TestComm testComm, List<TestClassRunInfo> tests) {

    if (tests.size() == 0) {
      return false;
    }

    for (int i = 0; i < tests.size(); i++) {
      TestClassRunInfo test = tests.get(i);

      testComm.getTestClasses().submittedEvent(test.getTestClassName());

      try {
        // lock.lock();
        testClassProcessor.processTestClass(test);
      } finally {
        // lock.unlock();
      }

      if (i < tests.size() - 2) {
        delayForProgress(testComm);
      }

      if (testComm.isStopNow()) {
        return true;
      }

    }

    return false;
  }

  private abstract class ClassFileVisitor extends EmptyFileVisitor {
    @Override
    public void visitFile(FileVisitDetails fileDetails) {
      if (isClass(fileDetails) && !isAnonymousClass(fileDetails)) {
        visitClassFile(fileDetails);
      }
    }

    abstract void visitClassFile(FileVisitDetails fileDetails);

    private boolean isAnonymousClass(FileVisitDetails fileVisitDetails) {
      return ANONYMOUS_CLASS_NAME.matcher(getClassName(fileVisitDetails)).matches();
    }

    private boolean isClass(FileVisitDetails fileVisitDetails) {
      return fileVisitDetails.getFile().getAbsolutePath().endsWith(".class");
    }
  }

  private String getClassName(FileVisitDetails fileDetails) {
    return fileDetails.getRelativePath().getPathString().replaceAll("\\.class", "").replace('/', '.');
  }

  private static class Result {
    boolean hasSlowAnnotation = false;
    boolean hasSlowestAnnotation = false;
    boolean hasNightlyAnnotation = false;
    boolean hasIgnoreAnnotation = false;
  }

  public class DynamicClassLoader extends ClassLoader {
    public Class<?> defineClass(String name, byte[] b) {
      return defineClass(name, b, 0, b.length);
    }
  }

  private Result readClassFile(File testClassFile, String className) {

    final TestVisitor classVisitor = new TestVisitor();

    InputStream classStream = null;
    try {
      classStream = new BufferedInputStream(new FileInputStream(testClassFile));

      final ClassReader classReader = new ClassReader(IOUtils.toByteArray(classStream));
      classReader.accept(classVisitor, ClassReader.SKIP_DEBUG | ClassReader.SKIP_CODE | ClassReader.SKIP_FRAMES);

      Result result = new Result();
      result.hasSlowAnnotation = classVisitor.hasSlowAnnotation();
      result.hasSlowestAnnotation = classVisitor.hasSlowestAnnotation();
      result.hasNightlyAnnotation = classVisitor.hasNightlyAnnotation();
      result.hasIgnoreAnnotation = classVisitor.hasIgnoreAnnotation();

      return result;
    } catch (Exception e) {
      log.error("Error reading class file:" + testClassFile, e);
      return new Result();
    } finally {
      IoActions.closeQuietly(classStream);
    }
  }

  private void delayForProgress(TestComm testComm) {
    try {

      if (testComm.isStopNow()) {
        return;
      }

      int outstanding = testComm.getOutstanding();

      // if (FastParallelTestExecuter.DEBUG)
      // System.out.println(testComm.getName() + " outstanding:" + outstanding + " submitted:"
      // + testComm.getTestClasses().getSubmitted());

      int loops = 0;

      if (loops < 100 && outstanding > testComm.getMaxOutstanding()) { // at the start we just don't want to send too
                                                                       // many too fast
        // testComm.pauseTests("For progress");
      } else if (testComm.getStartedOutstanding() > testComm.getMaxOutstanding()) { // now start considering outstanding
                                                                                    // based on what has started
        // testComm.pauseTests("For progress");
      }

      while (!testComm.isSendTests() && !testComm.isStopNow()) {

        loops++;
        if (loops % 100 == 0) {
          // if (FastParallelTestExecuter.DEBUG)
          // System.out.println(testComm.getName() + " outstanding:" + outstanding + " submitted:"
          // + testComm.getTestClasses().getSubmitted());
        }
        synchronized (testComm.getTestFlowLock()) {
          testComm.getTestFlowLock().wait(60000);
        }

        if (testComm.getStartedOutstanding() > testComm.getMaxOutstanding()
            && outstanding == testComm.getStartedOutstanding()) {
          testComm.pauseTests(
              "Was asked to release tests, but still have the same outstanding as before pause: " + outstanding);
        }
      }

      if (testComm.isStopNow()) {
        return;
      }

    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
