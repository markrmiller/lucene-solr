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

import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.Constants;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.util.RandomizeSSL.SSLRandomizer;
import org.apache.solr.util.SSLTestConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.RandomizedContext;

public class SSLSolrTestCase extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected static SSLTestConfig sslConfig;

  

  @BeforeClass
  public static void beforeSSLSolrTestCase() throws Exception {


    sslConfig = buildSSLConfig();
    // based on randomized SSL config, set SchemaRegistryProvider appropriately
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    if(isSSLMode()) {
      // SolrCloud tests should usually clear this
      System.setProperty("urlScheme", "https");
    }
  }
  
  @AfterClass
  public static void afterTestCases() throws Exception {
    sslConfig = null;
  }
  
  protected static boolean isSSLMode() {
    return sslConfig != null && sslConfig.isSSLMode();
  }
  
  private static SSLTestConfig buildSSLConfig() {

    SSLRandomizer sslRandomizer =
      SSLRandomizer.getSSLRandomizerForClass(RandomizedContext.current().getTargetClass());
    
    if (Constants.MAC_OS_X) {
      // see SOLR-9039
      // If a solution is found to remove this, please make sure to also update
      // TestMiniSolrCloudClusterSSL.testSslAndClientAuth as well.
      sslRandomizer = new SSLRandomizer(sslRandomizer.ssl, 0.0D, (sslRandomizer.debug + " w/ MAC_OS_X supressed clientAuth"));
    }

    SSLTestConfig result = sslRandomizer.createSSLTestConfig();
    log.info("Randomized ssl ({}) and clientAuth ({}) via: {}",
             result.isSSLMode(), result.isClientAuthMode(), sslRandomizer.debug);
    return result;
  }

}
