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

import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.TestInjection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests the Cloud Collections API.
 */
@LuceneTestCase.Slowest
public class CollectionsAPIDistDeleteZkTest extends SolrCloudTestCase {
  private static final String CLOUD_MINIMAL = "cloud-minimal";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected String getConfigSet() {
    return CLOUD_MINIMAL;
  }
  
  @BeforeClass
  public static void setupCluster() throws Exception {
    // we don't want this test to have zk timeouts
    System.setProperty("zkClientTimeout", "60000");
    System.setProperty("createCollectionWaitTimeTillActive", "3");
    if (TEST_NIGHTLY) TestInjection.randomDelayInCoreCreation = "true:5";
    System.setProperty("validateAfterInactivity", "200");
    if (TEST_NIGHTLY && random().nextBoolean()) useFactory(null);

    cluster = configureCluster(TEST_NIGHTLY ? 4 : 2)
        .addConfig("conf", configset(CLOUD_MINIMAL))
        .addConfig("conf2", configset(CLOUD_MINIMAL))
        .withSolrXml(TEST_PATH().resolve("solr.xml"))
        .configure();
  }

  @Before
  public void beforeCollectionsAPIDistributedZkTest() throws Exception {
  }
  
  @After
  public void after() throws Exception {
  }
  
  @AfterClass
  public static void tearDownCluster() throws Exception {
    try {
      shutdownCluster();
    } finally {
      System.clearProperty("createCollectionWaitTimeTillActive");
    }
  }

  @Test
  public void testCreationAndDeletion() throws Exception {
    String collectionName = "created_and_deleted";

    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1).process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient())
                  .contains(collectionName));

    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    waitForState("Timed out waiting for collection to be deleted", collectionName, (n, c) -> c == null);
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient())
        .contains(collectionName));

    assertFalse(cluster.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void deleteCollectionRemovesStaleZkCollectionsNode() throws Exception {
    String collectionName = "out_of_sync_collection";

    // manually create a collections zknode
    cluster.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    CollectionAdminRequest.deleteCollection(collectionName)
        .process(cluster.getSolrClient());

    cluster.waitForRemovedCollection(collectionName);
    
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient())
                  .contains(collectionName));
    
    assertFalse(cluster.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true));
  }

  @Test
  public void deletePartiallyCreatedCollection() throws Exception {
    final String collectionName = "halfdeletedcollection";

    assertEquals(0, CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .setCreateNodeSet("")
        .process(cluster.getSolrClient()).getStatus());
    cluster.waitForActiveCollection(collectionName, 2, 0);
    String dataDir = createTempDir().toFile().getAbsolutePath();
    // create a core that simulates something left over from a partially-deleted collection
    assertTrue(CollectionAdminRequest
        .addReplicaToShard(collectionName, "shard1")
        .setDataDir(dataDir)
        .process(cluster.getSolrClient()).isSuccess());
    
    cluster.waitForActiveCollection(collectionName, 2, 1);

    CollectionAdminRequest.deleteCollection(collectionName)
        .process(cluster.getSolrClient());

    cluster.waitForRemovedCollection(collectionName);
    
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collectionName, 2, 2);
    
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
  }

  @Test
  public void deleteCollectionOnlyInZk() throws Exception {
    final String collectionName = "onlyinzk";

    // create the collections node, but nothing else
    cluster.getZkClient().makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName, true);

    // delete via API - should remove collections node
    CollectionAdminRequest.deleteCollection(collectionName).process(cluster.getSolrClient());
    cluster.waitForRemovedCollection(collectionName);
    assertFalse(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));

    // now creating that collection should work
    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 2, 2);
    assertTrue(CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(collectionName));
  }

  @Test
  public void testBadActionNames() {
    // try a bad action
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    params.set("numShards", 2);
    final QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testMissingRequiredParameters() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", CollectionAction.CREATE.toString());
    params.set("numShards", 2);
    // missing required collection parameter
    final SolrRequest request = new QueryRequest(params);
    request.setPath("/admin/collections");

    expectThrows(Exception.class, () -> {
      cluster.getSolrClient().request(request);
    });
  }

  @Test
  public void testDeleteNonExistentCollection() throws Exception {

    expectThrows(SolrException.class, () -> {
      CollectionAdminRequest.deleteCollection("unknown_collection").process(cluster.getSolrClient());
    });

    // create another collection should still work
    CollectionAdminRequest.createCollection("acollectionafterbaddelete", "conf", 1, 2)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection("acollectionafterbaddelete", 1, 2);
  }

}
