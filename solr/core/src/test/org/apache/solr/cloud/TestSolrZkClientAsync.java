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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSolrZkClientAsync extends SolrTestCaseJ4 {
  
  private static final String THE_PATH_WITH_JSON = "/the/path/with/json";
  protected ZkTestServer zkServer;
  protected Path zkDir;
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    zkDir = createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    System.clearProperty("zkHost");

    if (null != zkServer) {
      zkServer.shutdown();
      zkServer = null;
    }
    zkDir = null;
    
    super.tearDown();
  }
  
  @Test
  public void testAsyncPathCreate() throws Exception {
    SolrZkClient zkClient = zkServer.chRootClient;
    
    zkClient.makePath("/mark", true);
    
    zkClient.mkDirs("/tom", "/hello/to/my/little/friend");
    
    assertTrue(zkClient.exists("/mark", true));
    assertTrue(zkClient.exists("/tom", true));
    assertTrue(zkClient.exists("/hello/to/my/little/friend", true));
    
    byte[] emptyJson = "{}".getBytes(StandardCharsets.UTF_8);
    Map<String,byte[]> dataMap = new HashMap<>();
    dataMap.put(THE_PATH_WITH_JSON, emptyJson);
    
    zkClient.mkDirs(dataMap);
    
    
   byte[] data = zkClient.getData(THE_PATH_WITH_JSON, null, null, true);
   
   assertArrayEquals("Expected to find our json!", emptyJson, data);
   
   
   zkClient.makePath("/solr", true);
   
   // should not fail
   zkClient.makePath("/solr/solr.xml", true, true);
   
  }


}
