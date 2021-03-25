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
package org.apache.solr.cloud;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SkyHook;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.update.processor.DistributedUpdateProcessor;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

@Slow
@SolrTestCase.SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
@LuceneTestCase.Nightly // pretty slow test
public class DistributedVersionInfoTest extends SolrCloudTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  private static final String COLLECTION = "c8n_vers_1x3";

  @Test
  public void testReplicaVersionHandling() throws Exception {

    final String shardId = "s1";

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 3)
        .process(cluster.getSolrClient());

    final ZkStateReader stateReader = cluster.getSolrClient().getZkStateReader();

    final Replica leader = stateReader.getLeaderRetry(COLLECTION, shardId, 1000, true);

    // start by reloading the empty collection so we try to calculate the max from an empty index
    reloadCollection(leader, COLLECTION);

    sendDoc(1);
    cluster.getSolrClient().commit(COLLECTION);

    // verify doc is on the leader and replica
    final List<Replica> notLeaders = stateReader.getClusterState().getCollection(COLLECTION).getReplicas()
        .stream()
        .filter(r -> r.getName().equals(leader.getName()) == false && stateReader.isNodeLive(r.getNodeName()) && r.getState() == Replica.State.ACTIVE)
        .collect(Collectors.toList());
    assertDocsExistInAllReplicas(leader, notLeaders, COLLECTION, 1, 1, null);

    // get max version from the leader and replica
    Replica replica = notLeaders.get(0);
    Long maxOnLeader = getMaxVersionFromIndex(leader);
    Long maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version: " + maxOnLeader, maxOnLeader, maxOnReplica);

    // send the same doc but with a lower version than the max in the index
    try (SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
      String docId = String.valueOf(1);
      SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", docId);
      doc.setField("_version_", maxOnReplica - 1); // bad version!!!

      // simulate what the leader does when sending a doc to a replica
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(DISTRIB_UPDATE_PARAM, DistributedUpdateProcessor.DistribPhase.FROMLEADER.toString());
      params.set(DISTRIB_FROM, leader.getCoreUrl());

      UpdateRequest req = new UpdateRequest();
      req.setParams(params);
      req.add(doc);

      log.info("Sending doc with out-of-date version ({}) document directly to replica", maxOnReplica -1);

      client.request(req);
      client.commit();

      Long docVersion = getVersionFromIndex(replica, docId);
      assertEquals("older version should have been thrown away", maxOnReplica, docVersion);
    }

    reloadCollection(leader, COLLECTION);

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version after reload", maxOnLeader, maxOnReplica);

    // now start sending docs while collection is reloading

    SolrTestCaseJ4.delQ("*:*");
    SolrTestCaseJ4.commit();

    final Set<Integer> deletedDocs = ConcurrentHashMap.newKeySet();
    final AtomicInteger docsSent = new AtomicInteger(0);
    final Random rand = new Random(5150);
    Thread docSenderThread = new Thread() {
      public void run() {

        // brief delay before sending docs
        try {
          Thread.sleep(rand.nextInt(30)+1);
        } catch (InterruptedException e) {
          log.error("Fail", e);
        }

        for (int i=1; i < (TEST_NIGHTLY ? 1000 : 100); i++) {
          if (i % (rand.nextInt(20)+1) == 0) {
            try {
              Thread.sleep(rand.nextInt(50)+1);
            } catch (InterruptedException e) {
              log.error("Fail", e);
            }
          }

          int docId = i+1;
          try {
            sendDoc(docId);
            docsSent.incrementAndGet();
          } catch (Exception e) {
            log.error("Fail id=" + docId, e);
          }
        }
      }
    };

    Thread reloaderThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(rand.nextInt(300)+1);
        } catch (InterruptedException e) {}

        for (int i=0; i < 3; i++) {
          try {
            reloadCollection(leader, COLLECTION);
          } catch (Exception e) {
            log.error("Fail", e);
          }

          try {
            Thread.sleep(rand.nextInt(300)+300);
          } catch (InterruptedException e) {}
        }
      }
    };

    Thread deleteThread = new Thread() {
      public void run() {

        // brief delay before sending docs
        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {}

        for (int i=0; i < 200; i++) {
          try {
            Thread.sleep(rand.nextInt(50)+1);
          } catch (InterruptedException e) {}

          int ds = docsSent.get();
          if (ds > 0) {
            int docToDelete = rand.nextInt(ds) + 1;
            if (!deletedDocs.contains(docToDelete)) {
              SolrTestCaseJ4.delI(String.valueOf(docToDelete));
              deletedDocs.add(docToDelete);
              if (SkyHook.skyHookDoc != null) {
                SkyHook.skyHookDoc.register(String.valueOf(docToDelete), "delete from test client");
              }
            }
          }
        }
      }
    };

    Thread committerThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(rand.nextInt(200)+1);
        } catch (InterruptedException e) {}

        for (int i=0; i < 20; i++) {
          try {
            cluster.getSolrClient().commit(COLLECTION);
          } catch (Exception e) {
            log.error("Fail", e);
          }

          try {
            Thread.sleep(rand.nextInt(100)+100);
          } catch (InterruptedException e) {}
        }
      }
    };


    docSenderThread.start();
    reloaderThread.start();
    committerThread.start();
    deleteThread.start();

    docSenderThread.join();
    reloaderThread.join();
    committerThread.join();
    deleteThread.join();

    cluster.getSolrClient().commit(COLLECTION);

    if (log.isInfoEnabled()) {
      log.info("Total of {} docs deleted", deletedDocs.size());
    }

    assertDocsExistInAllReplicas(leader, notLeaders, COLLECTION, 1, docsSent.get(), deletedDocs);

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version before reload", maxOnLeader, maxOnReplica);

    reloadCollection(leader, COLLECTION);

    maxOnLeader = getMaxVersionFromIndex(leader);
    maxOnReplica = getMaxVersionFromIndex(replica);
    assertEquals("leader and replica should have same max version after reload", maxOnLeader, maxOnReplica);

    assertDocsExistInAllReplicas(leader, notLeaders, COLLECTION, 1, TEST_NIGHTLY ? 1000 : 100, deletedDocs);

  }

  protected long getMaxVersionFromIndex(Replica replica) throws IOException, SolrServerException {
    return getVersionFromIndex(replica, null);
  }

  protected long getVersionFromIndex(Replica replica, String docId) throws IOException, SolrServerException {
    Long vers = null;
    String queryStr = (docId != null) ? "id:" + docId : "_version_:[0 TO *]";
    SolrQuery query = new SolrQuery(queryStr);
    query.setRows(1);
    query.setFields("id", "_version_");
    query.addSort(new SolrQuery.SortClause("_version_", SolrQuery.ORDER.desc));
    query.setParam("distrib", false);

    try (SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl())) {
      QueryResponse qr = client.query(query);
      SolrDocumentList hits = qr.getResults();
      if (hits.isEmpty())
        fail("No results returned from query: "+query);

      vers = (Long) hits.get(0).getFirstValue("_version_");
    }

    if (vers == null)
      fail("Failed to get version using query " + query + " from " + replica.getCoreUrl());

    return vers.longValue();
  }

  protected void assertDocsExistInAllReplicas(Replica leader, List<Replica> notLeaders,
                                              String testCollectionName,
                                              int firstDocId,
                                              int lastDocId,
                                              Set<Integer> deletedDocs)
      throws Exception {
    Http2SolrClient leaderSolr = getHttpSolrClient(leader);
    List<Http2SolrClient> replicas = new ArrayList<Http2SolrClient>(notLeaders.size());
    for (Replica r : notLeaders)
      replicas.add(getHttpSolrClient(r));

    try {
      for (int d = firstDocId; d <= lastDocId; d++) {

        if (deletedDocs != null && deletedDocs.contains(d))
          continue;

        String docId = String.valueOf(d);
        Long leaderVers = assertDocExists(leaderSolr, testCollectionName, docId, null);
        for (Http2SolrClient replicaSolr : replicas)
          assertDocExists(replicaSolr, testCollectionName, docId, leaderVers);
      }
    } finally {
      if (leaderSolr != null) {
        leaderSolr.close();
      }
      for (Http2SolrClient replicaSolr : replicas) {
        replicaSolr.close();
      }
    }
  }

  protected Http2SolrClient getHttpSolrClient(Replica replica) throws Exception {
    return SolrTestCaseJ4.getHttpSolrClient(replica.getCoreUrl());
  }

  protected void sendDoc(int docId) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", String.valueOf(docId));
    doc.addField("a_t", "hello" + docId);
    log.info("Send id={}", docId);
    if (SkyHook.skyHookDoc != null) {
      SkyHook.skyHookDoc.register(String.valueOf(docId), "send from test client");
    }
    AbstractFullDistribZkTestBase.sendDocs(cluster.getSolrClient(), COLLECTION, doc);
  }

  /**
   * Query the real-time get handler for a specific doc by ID to verify it
   * exists in the provided server, using distrib=false so it doesn't route to another replica.
   */
  @SuppressWarnings("rawtypes")
  protected Long assertDocExists(Http2SolrClient solr, String coll, String docId, Long expVers) throws Exception {
    QueryRequest qr = new QueryRequest(params("qt", "/get", "id", docId, "distrib", "false", "fl", "id,_version_"));
    NamedList rsp = solr.request(qr);
    SolrDocument doc = (SolrDocument)rsp.get("doc");
    String match = JSONTestUtil.matchObj("/id", doc, docId);
    assertTrue("Doc with id=" + docId + " not found in " + solr.getBaseURL() +
        " due to: " + match + "; rsp=" + rsp, match == null);

    Long vers = (Long)doc.getFirstValue("_version_");
    assertNotNull(vers);
    if (expVers != null)
      assertEquals("expected version of doc "+docId+" to be "+expVers, expVers, vers);

    return vers;
  }

  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    String coreName = replica.getName();
    boolean reloadedOk = false;
    try (Http2SolrClient client = SolrTestCaseJ4.getHttpSolrClient(replica.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      // send reload command for the collection
      log.info("Sending RELOAD command for {}", testCollectionName);
      CollectionAdminRequest.reloadCollection(testCollectionName)
          .process(client);
    }
    return reloadedOk;
  }
}
