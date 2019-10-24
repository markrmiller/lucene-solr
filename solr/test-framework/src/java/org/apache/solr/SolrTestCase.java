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

package org.apache.solr;

import static com.carrotsearch.randomizedtesting.RandomizedTest.systemPropertyAsBoolean;
import static org.apache.solr.cloud.SolrZkServer.ZK_WHITELIST_PROPERTY;

import java.io.File;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.CloseTimeTracker;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RandomizeSSL;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.TestInjection;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.TraceFormatting;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
@SuppressSysoutChecks(bugUrl = "Solr dumps tons of logs to console.")
@SuppressFileSystems("ExtrasFS") // might be ok, the failures with e.g. nightly runs might be "normal"
@RandomizeSSL()
@ThreadLeakLingering(linger = 0)
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40","Lucene41","Lucene42","Lucene45"})
public class SolrTestCase extends LuceneTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final List<String> DEFAULT_STACK_FILTERS = Arrays.asList(new String [] {
      "org.junit.",
      "junit.framework.",
      "sun.",
      "java.lang.reflect.",
      "com.carrotsearch.randomizedtesting.",
  });
  
  private static final int SOLR_TEST_TIMEOUT = Integer.getInteger("solr.test.timeout", 25);
  
  private static long testStartTime;
  
  private volatile static String interuptThreadWithNameContains;


  @ClassRule
  public static TestRule solrClassRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule())
             .around(new RevertDefaultThreadHandlerRule());
  
  
  /**
   * Annotation for test classes that want to disable ObjectReleaseTracker
   */
  @Documented
  @Inherited
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface SuppressObjectReleaseTracker {
    public String reason();
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link BeforeClass} method
   * @lucene.internal
   */
  @BeforeClass
  public static void beforeSolrTestCase() throws Exception {
    
    // this is the main thread running through the tests - give it max consideration
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
    
    // ant test -Dargs="-Dtests.force.assumption.failure.beforeclass=true"
    final String PROP = "tests.force.assumption.failure.beforeclass";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
    
    
    testStartTime = System.nanoTime();
    
    interuptThreadWithNameContains = null;
    
    
    
    
    System.setProperty(SolrTestCaseJ4.USE_NUMERIC_POINTS_SYSPROP, "true");
    System.setProperty("solr.tests.IntegerFieldType", "solr.IntPointField");
    System.setProperty("solr.tests.FloatFieldType", "solr.FloatPointField");
    System.setProperty("solr.tests.LongFieldType", "solr.LongPointField");
    System.setProperty("solr.tests.DoubleFieldType", "solr.DoublePointField");
    System.setProperty("solr.tests.DateFieldType", "solr.DatePointField");
    System.setProperty("solr.tests.EnumFieldType", "solr.EnumFieldType");
    
    System.setProperty("solr.test.useFilterForSortedQuery", "true");
    
    System.setProperty("solr.tests.numeric.dv", "true");
    System.setProperty("solr.tests.numeric.points", "true");
    System.setProperty("solr.tests.numeric.points.dv", "true");
    
    System.setProperty("solr.iterativeMergeExecIdleTime", "1000");
    System.setProperty("zookeeper.forceSync", "false");
    
    // test currently don't want this set universally
    // System.setProperty("managed.schema.mutable", "true");
    
    System.setProperty("solr.zkclienttimeout", "90000"); 
    
    System.setProperty("solr.httpclient.retries", "1");
    System.setProperty("solr.retries.on.forward", "1");
    System.setProperty("solr.retries.to.followers", "1");

    System.setProperty("solr.v2RealPath", "true");
    System.setProperty("zookeeper.forceSync", "no");
    System.setProperty("jetty.testMode", "true");
    System.setProperty("enable.update.log", usually() ? "true" : "false");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
    System.setProperty("solr.clustering.enabled", "false");
    System.setProperty("solr.peerSync.useRangeVersions", String.valueOf(random().nextBoolean()));
    System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "500");
    System.setProperty(ZK_WHITELIST_PROPERTY, "*");
    
    if (!TEST_NIGHTLY) {
      System.out.println("Set fast default for non @Nightly runs");
      // if not nightly, the default is stable and fast
      DirectUpdateHandler2.commitOnClose = false; // other tests turn this off and try to reset it - we use sys prop below to override
      
      // hdfs tests duplicate tons of our testing and starting hdfs clusters is expensive and slow, only run one non nightly
      System.setProperty("tests.disableHdfs", "true");
      
      System.setProperty("solr.maxContainerThreads", "20");
      System.setProperty("solr.lowContainerThreadsThreshold", "-1");
      System.setProperty("solr.minContainerThreads", "0");
      System.setProperty("solr.containerThreadsIdle", "30000");
      System.setProperty("evictIdleConnections", "20000");
      
      System.setProperty("solr.commitOnClose", "false"); // can make things quite slow
      System.setProperty("solr.codec", "solr.SchemaCodecFactory");
      System.setProperty("tests.COMPRESSION_MODE", "BEST_COMPRESSION");
      System.setProperty("tests.skipSetupCodec", "true");
      System.setProperty("solr.lock.type", "single");
      System.setProperty("solr.tests.lockType", "single");
      System.setProperty("solr.tests.mergePolicyFactory", "org.apache.solr.index.NoMergePolicyFactory");
      System.setProperty("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
      System.setProperty("solr.mscheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
      System.setProperty("bucketVersionLockTimeoutMs", "8000");
      System.setProperty("socketTimeout", "30000");
      System.setProperty("connTimeout", "10000");
      System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "0");
      System.setProperty("solr.cloud.starting-recovery-delay-milli-seconds", "0");
      
      System.setProperty("lucene.cms.override_core_count", "2");
      System.setProperty("lucene.cms.override_spins", "false");
      System.setProperty("solr.tests.maxBufferedDocs", "1000000");
      System.setProperty("solr.tests.ramBufferSizeMB", "20");
      System.setProperty("solr.tests.ramPerThreadHardLimitMB", "4");
      //System.setProperty("solr.disableJvmMetrics", "true");
      System.setProperty("useCompoundFile", "false");
      
      
      System.setProperty("prepRecoveryReadTimeoutExtraWait", "2000");
      System.setProperty("evictIdleConnections", "30000");
      System.setProperty("validateAfterInactivity", "-1");
      System.setProperty("leaderVoteWait", "1000");
      System.setProperty("leaderConflictResolveWait", "1000");
      

      System.setProperty("solr.recovery.recoveryThrottle", "1000");
      System.setProperty("solr.recovery.leaderThrottle", "500");
      
      System.setProperty("solr.cloud.wait-for-updates-with-stale-state-pause", "0");
      
      
      System.setProperty("solr.httpclient.retries", "1");
      System.setProperty("solr.retries.on.forward", "1");
      System.setProperty("solr.retries.to.followers", "1"); 

      useFactory("solr.RAMDirectoryFactory");
    }
    
    
    
    
  }
  
  public static void enableMetricsForNonNightly() {
    System.setProperty("solr.disableJvmMetrics", "false");
  }
  
  /** 
   * Special hook for sanity checking if any tests trigger failures when an
   * Assumption failure occures in a {@link Before} method
   * @lucene.internal
   */
  @Before
  public void checkSyspropForceBeforeAssumptionFailure() {
    // ant test -Dargs="-Dtests.force.assumption.failure.before=true"
    final String PROP = "tests.force.assumption.failure.before";
    assumeFalse(PROP + " == true",
                systemPropertyAsBoolean(PROP, false));
  }
  
  @AfterClass
  public static void afterSolrTestCase() throws Exception {
      try {
        TestInjection.notifyPauseForeverDone();
        try {

          if (suiteFailureMarker.wasSuccessful()) {
            // if the tests passed, make sure everything was closed / released
            if (!RandomizedContext.current().getTargetClass().isAnnotationPresent(SuppressObjectReleaseTracker.class)) {
              String orr = clearObjectTrackerAndCheckEmpty(0, false);
              assertNull(orr, orr);
              checkForInterruptRequest();
            } else {
              ObjectReleaseTracker.tryClose();
            }
          } else {
            ObjectReleaseTracker.tryClose();
          }
          resetFactory();

        } finally {
          ObjectReleaseTracker.clear();
          TestInjection.reset();
        }
      } finally {
        System.out.println("Show Close Times");
        Class<? extends Object> clazz = null;
        Long tooLongTime = 0L;
        try {
          synchronized (CloseTimeTracker.CLOSE_TIMES) {
            Map<String,CloseTimeTracker> closeTimes = CloseTimeTracker.CLOSE_TIMES;
            for (CloseTimeTracker closeTime : closeTimes.values()) {
//              if (closeTime.getClazz() == SolrCore.class) {
//                continue;
//              }
              if (closeTime.getElapsedMS() > 8000) {
                tooLongTime = closeTime.getElapsedMS();
                clazz = closeTime.getClazz();
              }
              closeTime.printCloseTimes();
              System.out.println("\n");
            }
          }

        } finally {
          CloseTimeTracker.CLOSE_TIMES.clear();
        }

        if (clazz != null) {
          fail("A " + clazz.getName() + " took too long to close: " + tooLongTime);
        }

        // Queue<CloseTimeTracker> ccCloseTimes = CoreContainer.CLOSE_TIMES;
        // tooLong = false;
        // tooLongTime = 0L;
        // for (CloseTimeTracker closeTime : ccCloseTimes) {
        // System.out.println("CoreContainer close time: " + closeTime + "ms");
        // if (closeTime.getElapsedMS() > 8000) {
        // tooLong = true;
        // tooLongTime = closeTime.getElapsedMS();
        // }
        // closeTime.printCloseTimes();
        // }
        //
        // if (tooLong) {
        // //fail("A CoreContainer took too long to close: " + tooLongTime);
        // }

        checkForInterruptRequest();
        long testTime = TimeUnit.SECONDS.convert(System.nanoTime() - testStartTime, TimeUnit.NANOSECONDS);
        if (!TEST_NIGHTLY && testTime > SOLR_TEST_TIMEOUT) {
          fail(
              "This test suite is too long for non @Nightly runs! Please improve it's performance, break it up, make parts of it @Nightly or make the whole suite @Nightly: "
                  + testTime);
        }
      }

    StartupLoggingUtils.shutdown();
  }
  
  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds) {
    return clearObjectTrackerAndCheckEmpty(waitSeconds, false);
  }
  
  /**
   * @return null if ok else error message
   */
  public static String clearObjectTrackerAndCheckEmpty(int waitSeconds, boolean tryClose) {
    
    for (Object object : ObjectReleaseTracker.OBJECTS.values()) {
      if (object instanceof SolrCore || object instanceof SolrIndexWriter) {
        IOUtils.closeQuietly(((SolrCore) object)); // core container doesn't wait for SolrCores created after load (would be slow anyway), IW who knows...
        waitSeconds = 15;
      }
      if (object instanceof SolrCore || object instanceof SolrIndexWriter) {
        IOUtils.closeQuietly(((SolrIndexWriter) object));
        waitSeconds = 15;
      }
    }
    
    TimeOut timeout = new TimeOut(waitSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    int retries = 0;
    String result;
    do {
      result = ObjectReleaseTracker.checkEmpty();
      if (result == null)
        break;
      
      try {
        if (retries % 5 == 0) {
          log.info("Waiting for all tracked resources to be released: " + ObjectReleaseTracker.OBJECTS);
          if (retries > 5) {
            printOriginStacks();
          }
        }
        TimeUnit.MILLISECONDS.sleep(500);
      } catch (InterruptedException e) { 
        // no, no, no
      }
    }
    while (!timeout.hasTimedOut());
    
    
    log.info("------------------------------------------------------- Done waiting for tracked resources to be released");
    
    ObjectReleaseTracker.clear();
    
    return result;
  }

  private static void printOriginStacks() {
    TraceFormatting tf = new TraceFormatting(DEFAULT_STACK_FILTERS);
    Map<Thread,StackTraceElement[]> stacksMap = Thread.getAllStackTraces();
    Set<Entry<Thread,StackTraceElement[]>> entries = stacksMap.entrySet();
    for (Entry<Thread,StackTraceElement[]> entry : entries) {
      String stack = tf.formatStackTrace(entry.getValue());
      System.err.println(entry.getKey().getName() + ":\n" + stack);
    }
  }
  
  private static boolean changedFactory = false;
  private static String savedFactory;
  /** Use a different directory factory.  Passing "null" sets to an FS-based factory */
  public static void useFactory(String factory) throws Exception {
    // allow calling more than once so a subclass can override a base class
    if (!changedFactory) {
      savedFactory = System.getProperty("solr.DirectoryFactory");
    }

    if (factory == null) {
      factory = random().nextInt(100) < 75 ? "solr.NRTCachingDirectoryFactory" : "solr.StandardDirectoryFactory"; // test the default most of the time
    }
    System.setProperty("solr.directoryFactory", factory);
    changedFactory = true;
  }

  public static void resetFactory() throws Exception {
    if (!changedFactory) return;
    changedFactory = false;
    if (savedFactory != null) {
      System.setProperty("solr.directoryFactory", savedFactory);
    } else {
      System.clearProperty("solr.directoryFactory");
    }
  }
  
  /** Gets a resource from the context classloader as {@link File}. This method should only be used,
   * if a real file is needed. To get a stream, code should prefer
   * {@link Class#getResourceAsStream} using {@code this.getClass()}.
   */
  public static File getFile(String name) {
    final URL url = SolrTestCaseJ4.class.getClassLoader().getResource(name.replace(File.separatorChar, '/'));
    if (url != null) {
      try {
        return new File(url.toURI());
      } catch (Exception e) {
        throw new RuntimeException("Resource was found on classpath, but cannot be resolved to a " + 
            "normal file (maybe it is part of a JAR file): " + name);
      }
    }
    final File file = new File(name);
    if (file.exists()) {
      return file;
    }
    throw new RuntimeException("Cannot find resource in classpath or in file-system (relative to CWD): " + name);
  }
  
  public static String TEST_HOME() {
    return getFile("solr/collection1").getParent();
  }

  public static Path TEST_PATH() { return getFile("solr/collection1").getParentFile().toPath(); }
  
  

  private static void checkForInterruptRequest() {
    try {
      String interruptThread = interuptThreadWithNameContains;
      if (interruptThread != null) {
        interruptThreadsOnTearDown(interruptThread, true);
        interuptThreadWithNameContains = null;
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
  

  // expert - for special cases
  public static void interruptThreadsOnTearDown(String nameContains, boolean now) {
    if (!now) {
      interuptThreadWithNameContains = nameContains;
      return;
    }
    
    System.out.println("DO FORCED INTTERUPTS");
    //  we need to filter and only do this for known threads? dont want users to count on this behavior unless necessary
    String testThread = Thread.currentThread().getName();
    //System.out.println("test thread:" + testThread);
    ThreadGroup tg = Thread.currentThread().getThreadGroup();
    //System.out.println("test group:" + tg.getName());
    Set<Entry<Thread,StackTraceElement[]>> threadSet = Thread.getAllStackTraces().entrySet();
    //System.out.println("thread count: " + threadSet.size());
    for (Entry<Thread,StackTraceElement[]> threadEntry : threadSet) {
      Thread thread = threadEntry.getKey();
      ThreadGroup threadGroup = thread.getThreadGroup();
      if (threadGroup != null) {
        if (threadGroup.getName().equals(tg.getName()) && !thread.getName().startsWith("SUITE") && !thread.getName().startsWith("Log4j2")) {
          interrupt(thread, nameContains);
          continue;
        }
      }
      
      while (threadGroup != null && threadGroup.getParent() != null && !thread.getName().startsWith("SUITE") && !thread.getName().startsWith("Log4j2")) {
        threadGroup = threadGroup.getParent();
        if (threadGroup.getName().equals(tg.getName())) {
          interrupt(thread, nameContains);
          continue;
        }
      }
    }
  }
  
  private static void interrupt(Thread thread, String nameContains) {
    if (thread.getName().contains(nameContains)) {
      System.out.println("do interrupt on " + thread.getName());
      thread.interrupt();
    }
  }
}
