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
import java.io.File;
import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class MinimalSolrCloudTest extends SolrCloudTestCase {

  private static final String COLLECTION_ONE_NAME = "collection1";
  private static final String COLLECTION_TWO_NAME = "collection2";

  private String solrUrl;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", new File(ExternalPaths.TECHPRODUCTS_CONFIGSET).toPath())
        .configure();
  }

  @Before
  public void createCollections() throws Exception {
    solrUrl = cluster.getJettySolrRunner(0).getBaseUrl().toString();

    CollectionAdminRequest.createCollection(COLLECTION_ONE_NAME, "conf", 1, 1).setMaxShardsPerNode(100).process(cluster.getSolrClient());
    CollectionAdminRequest.createCollection(COLLECTION_TWO_NAME, "conf", 10, 10).setMaxShardsPerNode(100).process(cluster.getSolrClient());
  }

  @After
  public void deleteCollections() throws Exception {
  //  cluster.deleteAllCollections();
  }

  @Test
  public void testEnsureDocumentsSentToCorrectCollection() throws Exception {
    int numTotalDocs = 1000;
    int numExpectedPerCollection = numTotalDocs / 2;
    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         SolrClient client = new ConcurrentUpdateHttp2SolrClient.Builder(solrUrl, http2Client)
        .withQueueSize(numTotalDocs).build()) {
      splitDocumentsAcrossCollections(client, numTotalDocs);

      assertEquals(numExpectedPerCollection, client.query(COLLECTION_ONE_NAME, new SolrQuery("*:*")).getResults().getNumFound());
      assertEquals(numExpectedPerCollection, client.query(COLLECTION_TWO_NAME, new SolrQuery("*:*")).getResults().getNumFound());
    }

  }

  private void splitDocumentsAcrossCollections(SolrClient client, int numTotalDocs) throws IOException, SolrServerException {
    for (int docNum = 0; docNum < numTotalDocs; docNum++) {
      final SolrInputDocument doc = new SolrInputDocument();
      doc.setField("id", "value" + docNum);

      if (docNum %2 == 0) {
        client.add(COLLECTION_ONE_NAME, doc);
      } else {
        client.add(COLLECTION_TWO_NAME, doc);
      }
    }

    client.commit(COLLECTION_ONE_NAME);
    client.commit(COLLECTION_TWO_NAME);
  }
}
