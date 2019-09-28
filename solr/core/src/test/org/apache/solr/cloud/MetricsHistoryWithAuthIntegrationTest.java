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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.TimeOut;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.cloud.MetricsHistoryIntegrationTest.createHistoryRequest;

/**
 * Tests that metrics history works even with Authentication enabled.
 * We test that the scheduled calls to /admin/metrics use PKI auth and therefore succeeds
 */
@LogLevel("org.apache.solr.handler.admin=DEBUG,org.apache.solr.security=DEBUG")
@LuceneTestCase.Slow
public class MetricsHistoryWithAuthIntegrationTest extends SolrCloudTestCase {

  private static SolrCloudManager cloudManager;
  private static SolrClient solrClient;
  private static final String SECURITY_JSON = "{\n" +
      "  'authentication':{\n" +
      "    'blockUnknown': false, \n" +
      "    'class':'solr.BasicAuthPlugin',\n" +
      "    'credentials':{'solr':'orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw='}},\n" +
      "  'authorization':{\n" +
      "    'class':'solr.RuleBasedAuthorizationPlugin',\n" +
      "    'user-role':{'solr':'admin'},\n" +
      "    'permissions':[{'name':'metrics','collection': null,'path':'/admin/metrics','role':'admin'},\n" +
      "      {'name':'metrics','collection': null,'path':'/api/cluster/metrics','role':'admin'}]}}";
  private static final CharSequence SOLR_XML_HISTORY_CONFIG =
      "<history>\n" +
      "  <str name=\"collectPeriod\">2</str>\n" +
      "</history>\n";

  @BeforeClass
  public static void setupCluster() throws Exception {
    enableMetricsForNonNightly();
    String solrXml = MiniSolrCloudCluster.DEFAULT_CLOUD_SOLR_XML.replace("<metrics>\n",
        "<metrics>\n" + SOLR_XML_HISTORY_CONFIG);
    // Spin up a cluster with a protected /admin/metrics handler, and a 2 seconds metrics collectPeriod
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .withSolrXml(solrXml)
        .configure();
    cloudManager = cluster.getJettySolrRunner(0).getCoreContainer().getZkController().getSolrCloudManager();
    solrClient = cluster.getSolrClient();
  }

  @AfterClass
  public static void teardown() {
    solrClient = null;
    cloudManager = null;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testValuesAreCollected() throws Exception {
    NamedList<Object> rsp = null;
    // default format is LIST
    NamedList<Object> data = null;

    // Has actual values. These will be 0.0 if metrics could not be collected
    NamedList<Object> memEntry;
    List<Double> heap = null;

    TimeOut timeout = new TimeOut(20, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (!timeout.hasTimedOut()) {
      rsp = solrClient.request(createHistoryRequest(params(
          CommonParams.ACTION, "get", CommonParams.NAME, "solr.jvm")));
      if (rsp != null) {
        // default format is LIST
        data = (NamedList<Object>) rsp.findRecursive("metrics", "solr.jvm", "data");
        if (data != null) {

          // Has actual values. These will be 0.0 if metrics could not be collected
          memEntry = (NamedList<Object>) ((NamedList<Object>) data.iterator().next().getValue()).get("values");
          heap = (List<Double>) memEntry.getAll("memory.heap.used").get(0);
          if (heap.get(240) != null && heap.get(240) > .01) {
            break;
          }
        }
      }
      cloudManager.getTimeSource().sleep(1000);
    }

    assertNotNull(rsp);
    assertNotNull(rsp.toString(), data);

    assertTrue("Expected memory.heap.used > 0 in history", heap.get(240) > 0.01);
  }
}