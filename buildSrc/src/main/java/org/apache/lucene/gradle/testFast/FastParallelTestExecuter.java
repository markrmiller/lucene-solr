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

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.gradle.api.file.FileTree;
import org.gradle.api.internal.DocumentationRegistry;
import org.gradle.api.internal.classpath.ModuleRegistry;
import org.gradle.api.internal.tasks.testing.JvmTestExecutionSpec;
import org.gradle.api.internal.tasks.testing.TestClassProcessor;
import org.gradle.api.internal.tasks.testing.TestExecuter;
import org.gradle.api.internal.tasks.testing.TestFramework;
import org.gradle.api.internal.tasks.testing.TestResultProcessor;
import org.gradle.api.internal.tasks.testing.WorkerTestClassProcessorFactory;
import org.gradle.api.internal.tasks.testing.filter.DefaultTestFilter;
import org.gradle.api.internal.tasks.testing.processors.PatternMatchTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.RestartEveryNTestClassProcessor;
import org.gradle.api.internal.tasks.testing.processors.TestMainAction;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.internal.Factory;
import org.gradle.internal.actor.ActorFactory;
import org.gradle.internal.operations.BuildOperationExecutor;
import org.gradle.internal.time.Clock;
import org.gradle.internal.work.WorkerLeaseRegistry;
import org.gradle.process.internal.worker.WorkerProcessFactory;

/**
 * The default test class scanner factory.
 */
public class FastParallelTestExecuter implements TestExecuter<JvmTestExecutionSpec> {

  private static final int FAST_JVM_COUNT = 0;

  private static final Logger log = Logging.getLogger(FastParallelTestExecuter.class);

  // TODO switch to logger
  public static final boolean DEBUG = false;
  public static boolean ANNOTATION_HINTS = false;

  private final WorkerProcessFactory workerFactory;
  private final ActorFactory actorFactory;
  private final ModuleRegistry moduleRegistry;
  private final WorkerLeaseRegistry workerLeaseRegistry;
  private final BuildOperationExecutor buildOperationExecutor;
  private final int maxWorkerCount;
  private final Clock clock;
  private final DocumentationRegistry documentationRegistry;
  private final DefaultTestFilter testFilter;
  private TestClassProcessor processor;

  public FastParallelTestExecuter(WorkerProcessFactory workerFactory, ActorFactory actorFactory,
      ModuleRegistry moduleRegistry,
      WorkerLeaseRegistry workerLeaseRegistry, BuildOperationExecutor buildOperationExecutor, int maxWorkerCount,
      Clock clock, DocumentationRegistry documentationRegistry, DefaultTestFilter testFilter) {
    this.workerFactory = workerFactory;
    this.actorFactory = actorFactory;
    this.moduleRegistry = moduleRegistry;
    this.workerLeaseRegistry = workerLeaseRegistry;
    this.buildOperationExecutor = buildOperationExecutor;
    this.maxWorkerCount = maxWorkerCount;
    this.clock = clock;
    this.documentationRegistry = documentationRegistry;
    this.testFilter = testFilter;
  }

  @Override
  public void execute(final JvmTestExecutionSpec testExecutionSpec, TestResultProcessor testResultProcessor) {
    try {
      final TestFramework testFramework = testExecutionSpec.getTestFramework();
      final WorkerTestClassProcessorFactory testInstanceFactory = testFramework.getProcessorFactory();
      final WorkerLeaseRegistry.WorkerLease currentWorkerLease = workerLeaseRegistry.getCurrentWorkerLease();

      Set<File> classpath = new HashSet<>();
      for (File file : testExecutionSpec.getClasspath()) {
        classpath.add(file);
      }

      final Factory<TestClassProcessor> forkingProcessorFactory = new Factory<TestClassProcessor>() {
        @Override
        public TestClassProcessor create() {
          return new ForkingTestClassProcessor(currentWorkerLease, workerFactory, testInstanceFactory,
              testExecutionSpec.getJavaForkOptions(),
              classpath, testFramework.getWorkerConfigurationAction(), moduleRegistry, documentationRegistry);
        }
      };
      final Factory<TestClassProcessor> reforkingProcessorFactory = new Factory<TestClassProcessor>() {
        @Override
        public TestClassProcessor create() {
          return new RestartEveryNTestClassProcessor(forkingProcessorFactory, testExecutionSpec.getForkEvery());
        }
      };

      int maxJvms = getMaxParallelForks(testExecutionSpec);

      int fastJvms = FAST_JVM_COUNT;
      int slowJvms = maxJvms - fastJvms;

      final TestComm testCommSlow = new TestComm("Slow", slowJvms, slowJvms * 3, 15, true); // max outstanding, cost

      TestComm testCommFast;
      if (fastJvms > 0) {
        testCommFast = new TestComm("Fast", fastJvms, fastJvms * 15, fastJvms * 6, false, 1.0);
      } else {
        testCommFast = null;
      }

      JVMTestQueues slowJVMs;
      JVMTestQueues fastJVMs;

      slowJVMs = new JVMTestQueues(slowJvms, testCommSlow, reforkingProcessorFactory, actorFactory);

      if (fastJvms > 0) {
        fastJVMs = new JVMTestQueues(fastJvms, testCommFast, reforkingProcessorFactory, actorFactory);
      } else {
        fastJVMs = null;
      }

      TestResultProcessorWrapper trp = new TestResultProcessorWrapper(testResultProcessor, testCommSlow, testCommFast,
          slowJVMs, fastJVMs);

      processor = new PatternMatchTestClassProcessor(testFilter, new MaxNParallelTestClassProcessor(
          maxJvms, reforkingProcessorFactory, actorFactory, testCommSlow, testCommFast, slowJVMs, fastJVMs));

      final FileTree testClassFiles = testExecutionSpec.getCandidateClassFiles();

      Runnable detector;

      detector = new FastTestClassScanner(testClassFiles, processor, testFilter, testCommSlow, testCommFast, maxJvms);

      final Object testTaskOperationId = buildOperationExecutor.getCurrentOperation().getParentId();

      new TestMainAction(detector, processor, trp, clock, testTaskOperationId, testExecutionSpec.getPath(),
          "Gradle Test Run " + testExecutionSpec.getIdentityPath()).run();

    } catch (Exception e) {
      log.error("Error in execute", e);
      throw e;
    }
  }

  @Override
  public void stopNow() {
    processor.stopNow();
  }

  private int getMaxParallelForks(JvmTestExecutionSpec testExecutionSpec) {
    int maxParallelForks = testExecutionSpec.getMaxParallelForks();
    if (maxParallelForks > maxWorkerCount) {
      log.info("{}.maxParallelForks ({}) is larger than max-workers ({}), forcing it to {}",
          testExecutionSpec.getPath(), maxParallelForks, maxWorkerCount, maxWorkerCount);
      maxParallelForks = maxWorkerCount;
    }
    return maxParallelForks;
  }
}
