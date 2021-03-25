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
package org.apache.solr.cloud.api.collections;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.StopWatch;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slow
@LuceneTestCase.Nightly
//@Ignore
public class CreateCollectionsIndexAndRestartTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int RECOVERY_WAIT = 15;

  @BeforeClass
  public static void beforeCreateCollectionsIndexAndRestartTest() throws Exception {
   // checkInterruptsOnFinish = false;
    System.setProperty("solr.containerThreadsIdleTimeout", "100");
    System.setProperty("solr.minContainerThreads", "8");

    System.setProperty("solr.enablePublicKeyHandler", "false");
    System.setProperty("zookeeper.nio.numSelectorThreads", "16");
    System.setProperty("zookeeper.nio.numWorkerThreads", "32");
    System.setProperty("zookeeper.commitProcessor.numWorkerThreads", "16");
    System.setProperty("zookeeper.admin.enableServer", "false");
    System.setProperty("zookeeper.skipACL", "true");
    System.setProperty("zookeeper.nio.directBufferBytes", Integer.toString(128000));
    System.setProperty("solr.zkclienttimeout", "45000");
    System.setProperty("solr.getleader.looptimeout", "20000");
    System.setProperty("disableCloseTracker", "true");
    System.setProperty("solr.rootSharedThreadPoolCoreSize", "256");
    System.setProperty("solr.v2RealPath", "false");


    useFactory(null);
    configureCluster(2)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  @AfterClass
  public static void afterCreateCollectionsIndexAndRestartTest() throws Exception {
    shutdownCluster();
  }

  @Test
  public void start() throws Exception {
    int collectionCnt = 20;
    int numShards = 2;
    int numReplicas = 2;
    System.err.println(new Date() + " ********* CREATING " + collectionCnt + " 4x4 COLLECTIONS");
    StopWatch stopWatch = new StopWatch(true);
    stopWatch.start("Create Collections");
    Set<Future> futures = ConcurrentHashMap.newKeySet(collectionCnt);
    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      Future<?> future = ParWork.getRootSharedExecutor().submit(() -> {
        try {
          //System.err.println(new Date() + " - Create Collection " + collectionName);
          CollectionAdminRequest.createCollection(collectionName, "conf", numShards, numReplicas).process(cluster.getSolrClient());

        } catch (Exception e) {
          log.error("", e);
        }
      });
      futures.add(future);
    }

    for (Future future : futures) {
      future.get(240, TimeUnit.SECONDS);
    }

    stopWatch.done();
    System.err.println(new Date() + " ********* CREATING " + collectionCnt + " COLLECTIONS DONE, WAITING FOR FULLY ACTIVE STATES: " + stopWatch.getTime() + "ms");
    stopWatch.start("Wait for active states");

    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      cluster.waitForActiveCollection(collectionName, RECOVERY_WAIT, TimeUnit.SECONDS, numShards, numReplicas * numShards);
    }
    stopWatch.done();
    System.err.println(new Date() + " ********* " + collectionCnt + " COLLECTIONS FOUND FULLY ACTIVE: " + stopWatch.getTime() + "ms");
    stopWatch.start("Stop Jetty Instances");
    System.err.println(new Date() + " ********* RANDOMLY RESTARTING JETTY INSTANCES");
    Set<JettySolrRunner> stoppedRunners = ConcurrentHashMap.newKeySet();

    try (ParWork work = new ParWork(this)) {
      for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
        if (random().nextBoolean()) {
          continue;
        }
        System.err.println(new Date() + " ********* STOPPING " + runner.getBaseUrl());
        stoppedRunners.add(runner);
        work.collect("", () -> {
          try {
            runner.stop();
          } catch (Exception e) {
            log.error("", e);
          }
        });
      }
    }

    stopWatch.done();

    System.err.println(new Date() + " ********* DONE RANDOMLY STOPING JETTY INSTANCES: " + stopWatch.getTime() + "ms");

    stopWatch.start("Starting Jetty instances");

    try (ParWork work = new ParWork(this)) {
      for (JettySolrRunner runner : stoppedRunners) {
        System.err.println(new Date() + " ********* STARTING " + runner.getBaseUrl());
        work.collect("", () -> {
          try {
            runner.start();
          } catch (Exception e) {
            log.error("", e);
          }
        });
      }
    }
    stopWatch.done();
    System.err.println(new Date() + " ********* DONE STARTING JETTY INSTANCES: " + stopWatch.getTime() + "ms");

//    System.err.println(new Date() + " ********* WAITING 5 SECONDS");
//    Thread.sleep(5000);

    System.err.println(new Date() + " ********* WAITING TO SEE " + collectionCnt + " FULLY ACTIVE COLLECTIONS");
    stopWatch.start("Wait for ACTIVE collections");

    for (int i = 1; i <= collectionCnt; i++) {
      final String collectionName = "testCollection" + i;
      cluster.waitForActiveCollection(cluster.getSolrClient().getHttpClient(), collectionName, RECOVERY_WAIT, TimeUnit.SECONDS, false, numShards, numShards * numReplicas, true, true);
    }
//Http2SolrClient client, String collection, long wait, TimeUnit unit, boolean justLeaders, int shards, int totalReplicas, boolean exact, boolean verifyLeaders)
    stopWatch.done();
    System.err.println(new Date() + " *********" + collectionCnt + " COLLECTIONS FOUND FULLY ACTIVE: " + stopWatch.getTime() + "ms");
  }

}
