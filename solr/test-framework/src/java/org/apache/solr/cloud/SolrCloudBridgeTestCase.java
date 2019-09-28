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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStatePredicate;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.RestTestHarness;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SolrCloudBridgeTestCase extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static String COLLECTION = "collection1";
  protected static String  DEFAULT_COLLECTION = COLLECTION;

  protected static CloudSolrClient cloudClient;
  
  protected static final String SHARD1 = "shard1";
  
  protected String id = "id";

  private static final List<SolrClient> newClients = Collections.synchronizedList(new ArrayList<>());
  
  protected Map<String, Integer> handle = new ConcurrentHashMap<>();
  
  private static final List<RestTestHarness> restTestHarnesses = Collections.synchronizedList(new ArrayList<>());
  
  public final static int ORDERED = 1;
  public final static int SKIP = 2;
  public final static int SKIPVAL = 4;
  public final static int UNORDERED = 8;

  String t1="a_t";
  String i1="a_i1";
  String tlong = "other_tl1";
  String tsort="t_sortable";

  String oddField="oddField_s";
  String missingField="ignore_exception__missing_but_valid_field_t";

  public static RandVal rdate = new RandDate();
  
  protected static String[] fieldNames = new String[]{"n_ti1", "n_f1", "n_tf1", "n_d1", "n_td1", "n_l1", "n_tl1", "n_dt1", "n_tdt1"};
  
  protected static int numShards = 3;
  
  protected static int sliceCount = 2;
  
  protected static int replicationFactor = 1;
  
  protected final List<SolrClient> clients = new ArrayList<>();
  protected volatile static boolean createControl;
  protected volatile static CloudSolrClient controlClient;
  private volatile static MiniSolrCloudCluster controlCluster;
  protected volatile static String schemaString;
  protected volatile static String solrconfigString;
  
  public static Path TEST_PATH() { return SolrTestCaseJ4.getFile("solr/collection1").getParentFile().toPath(); }
  
  @Before
  public void beforeSolrCloudBridgeTestCase() throws Exception {
    
    System.out.println("Before Bridge");
    System.setProperty("solr.test.sys.prop1", "propone");
    System.setProperty("solr.test.sys.prop2", "proptwo");
    
    System.out.println("Make cluster with shard count:" + numShards);
    
    cluster = configureCluster(numShards).build();
    
    SolrZkClient zkClient = cluster.getZkClient();
    
    Pattern filenameExclusions = Pattern.compile(".*solrconfig(?:-|_).*?\\.xml|.*schema(?:-|_).*?\\.xml");
    zkClient.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf"), "configs" + "/" + "conf1", filenameExclusions);
    
    zkClient.printLayoutToStream(System.out);
    
    
    if (schemaString != null) {
      //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString), "/configs/conf1", null);
      
      zkClient.setData("/configs/conf1/schema.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
      byte[] data = FileUtils.readFileToByteArray(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile());
      zkClient.create("/configs/conf1/managed-schema", data, CreateMode.PERSISTENT, true);
    }
    if (solrconfigString != null) {
      //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/conf1", null);
      zkClient.setData("/configs/conf1/solrconfig.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
    }
    
    CollectionAdminRequest.createCollection(COLLECTION, "conf1", sliceCount, replicationFactor)
        .setMaxShardsPerNode(10)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, sliceCount, sliceCount * replicationFactor);

    cloudClient = cluster.getSolrClient();
    cloudClient.setDefaultCollection(COLLECTION);
    
    
    for (int i =0;i < cluster.getJettySolrRunners().size(); i++) {
      clients.add(getClient(i));
    }
    
    if (createControl) {
      controlCluster = configureCluster(1).build();
      
      SolrZkClient zkClientControl = controlCluster.getZkClient();
      
      zkClientControl.uploadToZK(TEST_PATH().resolve("collection1").resolve("conf"), "configs" + "/" + "conf1", filenameExclusions);
      
      zkClientControl.printLayoutToStream(System.out);
      
      
      if (schemaString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString), "/configs/conf1", null);
        
        zkClientControl.setData("/configs/conf1/schema.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile(), true);
        byte[] data = FileUtils.readFileToByteArray(TEST_PATH().resolve("collection1").resolve("conf").resolve(schemaString).toFile());
        zkClientControl.create("/configs/conf1/managed-schema", data, CreateMode.PERSISTENT, true);
      }
      if (solrconfigString != null) {
        //cloudClient.getZkStateReader().getZkClient().uploadToZK(TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString), "/configs/conf1", null);
        zkClientControl.setData("/configs/conf1/solrconfig.xml", TEST_PATH().resolve("collection1").resolve("conf").resolve(solrconfigString).toFile(), true);
      }
      CollectionAdminRequest.createCollection(COLLECTION, "conf1", 1, 1)
          .setMaxShardsPerNode(10)
          .process(controlCluster.getSolrClient());
      controlCluster.waitForActiveCollection(COLLECTION, 1, 1);

      controlClient = controlCluster.getSolrClient();
      controlClient.setDefaultCollection(COLLECTION);
    }
  }
  
  @After
  public void cleanup() throws Exception {
    if (cluster != null) cluster.shutdown();
    if (controlCluster != null) controlCluster.shutdown();
  }
  
  
  @AfterClass
  public static void afterSolrCloudBridgeTestCase() throws Exception {
    synchronized (newClients) {
      for (SolrClient client : newClients) {
        client.close();
      }
    }
    
    closeRestTestHarnesses();
  }
  
  protected String getBaseUrl(HttpSolrClient client) {
    return client .getBaseURL().substring(
        0, client.getBaseURL().length()
            - DEFAULT_COLLECTION.length() - 1);
  }
  
  protected String getShardsString() {
    StringBuilder sb = new StringBuilder();
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (sb.length() > 0) sb.append(',');
      sb.append(runner.getBaseUrl() + "/" + DEFAULT_COLLECTION);
    }

    return sb.toString();
  }
  
  public HttpSolrClient getClient(int i) {
    return getClient(DEFAULT_COLLECTION, i);
  }
  
  public HttpSolrClient getClient(String collection, int i) {
    String baseUrl = cluster.getJettySolrRunner(i).getBaseUrl().toString() + "/" + collection;
    HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
        .withConnectionTimeout(15)
        .withSocketTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }
  
  public HttpSolrClient getClient(String collection, String url) {
    String baseUrl = url + "/" + collection;
    HttpSolrClient client = new HttpSolrClient.Builder(baseUrl)
        .withConnectionTimeout(15)
        .withSocketTimeout(Integer.getInteger("socketTimeout", 30000))
        .build();
    newClients.add(client);
    return client;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "conf1", numShards, numReplicas)
        .setMaxShardsPerNode(10)
        .setCreateNodeSet(null)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr, String routerField) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "conf1", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(routerField)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr, String routerField, String conf) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, conf, numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setRouterField(routerField)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    return resp;
  }
  
  protected CollectionAdminResponse createCollection(String collectionName, int numShards, int numReplicas, int maxShardsPerNode, String createNodeSetStr) throws SolrServerException, IOException {
    CollectionAdminResponse resp = CollectionAdminRequest.createCollection(collectionName, "conf1", numShards, numReplicas)
        .setMaxShardsPerNode(maxShardsPerNode)
        .setCreateNodeSet(createNodeSetStr)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    return resp;
  }
  
  protected void index(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    indexDoc(doc);
  }
  
  protected void index_specific(int serverNumber, Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);

    SolrClient client = clients.get(serverNumber);
    client.add(doc);
  }
  
  protected void index_specific(SolrClient client, Object... fields)
      throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }

    UpdateRequest ureq = new UpdateRequest();
    ureq.add(doc);
    // ureq.setParam("update.chain", DISTRIB_UPDATE_CHAIN);
    ureq.process(client);

    // add to control second in case adding to shards fails
    controlClient.add(doc);
  }
  
  protected Replica getShardLeader(String testCollectionName, String shardId, int timeoutSecs) throws Exception {
    Replica leader = null;
    long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutSecs, TimeUnit.SECONDS);
    while (System.nanoTime() < timeout) {
      Replica tmp = null;
      try {
        tmp = cloudClient.getZkStateReader().getLeaderRetry(testCollectionName, shardId);
      } catch (Exception exc) {}
      if (tmp != null && "active".equals(tmp.getStr(ZkStateReader.STATE_PROP))) {
        leader = tmp;
        break;
      }
      Thread.sleep(300);
    }
    assertNotNull("Could not find active leader for " + shardId + " of " +
        testCollectionName + " after "+timeoutSecs+" secs;", leader);

    return leader;
  }
  
  protected JettySolrRunner getJettyOnPort(int port) {
    JettySolrRunner theJetty = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (port == jetty.getLocalPort()) {
        theJetty = jetty;
        break;
      }
    }

    if (createControl) {
      if (theJetty == null) {
        if (controlCluster.getJettySolrRunner(0).getLocalPort() == port) {
          theJetty = controlCluster.getJettySolrRunner(0);
        }
      }
    }
    if (theJetty == null)
      fail("Not able to find JettySolrRunner for port: "+port);

    return theJetty;
  }
  
  public static void commit() throws SolrServerException, IOException {
    if (controlClient != null) controlClient.commit();
    cloudClient.commit();
  }
  
  protected int getShardCount() {
    return numShards;
  }
  
  public static abstract class RandVal {
    public static Set uniqueValues = new HashSet();

    public abstract Object val();

    public Object uval() {
      for (; ;) {
        Object v = val();
        if (uniqueValues.add(v)) return v;
      }
    }
  }
  
  protected void setDistributedParams(ModifiableSolrParams params) {
    params.set("shards", getShardsString());
  }
  
  protected QueryResponse query(SolrParams p) throws Exception {
    return query(true, p);
  }
  
  protected QueryResponse query(boolean setDistribParams, SolrParams p) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams(p);

    // TODO: look into why passing true causes fails
    //params.set("distrib", "false");
    //final QueryResponse controlRsp = controlClient.query(params);
    //validateControlData(controlRsp);

    //params.remove("distrib");
    if (setDistribParams) setDistributedParams(params);

    QueryResponse rsp = queryServer(params);

    //compareResponses(rsp, controlRsp);

    return rsp;
  }
  
  protected QueryResponse query(boolean setDistribParams, Object[] q) throws Exception {
    
    final ModifiableSolrParams params = new ModifiableSolrParams();

    for (int i = 0; i < q.length; i += 2) {
      params.add(q[i].toString(), q[i + 1].toString());
    }
    return query(setDistribParams, params);
  }
  
  protected QueryResponse queryServer(ModifiableSolrParams params) throws Exception {
    return cloudClient.query(params);
  }
  
  protected QueryResponse query(Object... q) throws Exception {
    return query(true, q);
  }
  
  protected void indexr(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    addFields(doc, "rnd_b", true);
    addRandFields(doc);
    indexDoc(doc);
  }
  
  protected UpdateResponse indexDoc(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateResponse specificRsp = add(cloudClient, params, sdocs);
    return specificRsp;
  }

  protected UpdateResponse add(SolrClient client, SolrParams params, SolrInputDocument... sdocs) throws IOException, SolrServerException {
    UpdateRequest ureq = new UpdateRequest();
    ureq.setParams(new ModifiableSolrParams(params));
    for (SolrInputDocument sdoc : sdocs) {
      ureq.add(sdoc);
    }
    return ureq.process(client);
  }
  
  protected static void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }

  public static Object[] getRandFields(String[] fields, RandVal[] randVals) {
    Object[] o = new Object[fields.length * 2];
    for (int i = 0; i < fields.length; i++) {
      o[i * 2] = fields[i];
      o[i * 2 + 1] = randVals[i].uval();
    }
    return o;
  }
  
  protected SolrInputDocument addRandFields(SolrInputDocument sdoc) {
    addFields(sdoc, getRandFields(fieldNames, randVals));
    return sdoc;
  }
  
  protected SolrInputDocument getDoc(Object... fields) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    addFields(doc, fields);
    return doc;
  }
  
  protected void indexDoc(SolrInputDocument doc) throws IOException, SolrServerException {
    if (controlClient != null) controlClient.add(doc);
    cloudClient.add(doc);
  }
  
  protected void del(String query) throws SolrServerException, IOException {
    if (controlClient != null) controlClient.deleteByQuery(query);
    cloudClient.deleteByQuery(query);
  }

  protected void waitForRecoveriesToFinish(String collectionName) throws InterruptedException, TimeoutException {
    cloudClient.getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, new AllActive());
  }
  
  protected void waitForRecoveriesToFinish() throws InterruptedException, TimeoutException {
    waitForRecoveriesToFinish(DEFAULT_COLLECTION);
  }
  
  protected ZkCoreNodeProps getLeaderUrlFromZk(String collection, String slice) {
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    ZkNodeProps leader = clusterState.getCollection(collection).getLeader(slice);
    if (leader == null) {
      throw new RuntimeException("Could not find leader:" + collection + " " + slice);
    }
    return new ZkCoreNodeProps(leader);
  }
  
  /**
   * Create a collection in single node
   */
  protected void createCollectionInOneInstance(final SolrClient client, String nodeName,
                                               ThreadPoolExecutor executor, final String collection,
                                               final int numShards, int numReplicas) {
    assertNotNull(nodeName);
    try {
      assertEquals(0, CollectionAdminRequest.createCollection(collection, "conf1", numShards, 1)
          .setCreateNodeSet("")
          .process(client).getStatus());
    } catch (SolrServerException | IOException e) {
      throw new RuntimeException(e);
    }
    for (int i = 0; i < numReplicas; i++) {
      final int freezeI = i;
      executor.execute(() -> {
        try {
          assertTrue(CollectionAdminRequest.addReplicaToShard(collection, "shard"+((freezeI%numShards)+1))
              .setCoreName(collection + freezeI)
              .setNode(nodeName).process(client).isSuccess());
        } catch (SolrServerException | IOException e) {
          throw new RuntimeException(e);
        }
      });
    }
    cluster.waitForActiveCollection(collection, numShards, numReplicas);
  }
  
  protected boolean reloadCollection(Replica replica, String testCollectionName) throws Exception {
    ZkCoreNodeProps coreProps = new ZkCoreNodeProps(replica);
    String coreName = coreProps.getCoreName();
    boolean reloadedOk = false;
    try (HttpSolrClient client = getHttpSolrClient(coreProps.getBaseUrl())) {
      CoreAdminResponse statusResp = CoreAdminRequest.getStatus(coreName, client);
      long leaderCoreStartTime = statusResp.getStartTime(coreName).getTime();

      // send reload command for the collection
      log.info("Sending RELOAD command for "+testCollectionName);
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("action", CollectionParams.CollectionAction.RELOAD.toString());
      params.set("name", testCollectionName);
      QueryRequest request = new QueryRequest(params);
      request.setPath("/admin/collections");
      client.request(request);

      // verify reload is done, waiting up to 30 seconds for slow test environments
      long timeout = System.nanoTime() + TimeUnit.NANOSECONDS.convert(30, TimeUnit.SECONDS);
      while (System.nanoTime() < timeout) {
        statusResp = CoreAdminRequest.getStatus(coreName, client);
        long startTimeAfterReload = statusResp.getStartTime(coreName).getTime();
        if (startTimeAfterReload > leaderCoreStartTime) {
          reloadedOk = true;
          break;
        }
        // else ... still waiting to see the reloaded core report a later start time
        Thread.sleep(1000);
      }
    }
    return reloadedOk;
  }
  
  protected void setupRestTestHarnesses() {
    for (final SolrClient client : clients) {
      RestTestHarness harness = new RestTestHarness(() -> ((HttpSolrClient) client).getBaseURL());
      restTestHarnesses.add(harness);
    }
  }

  protected static void closeRestTestHarnesses() throws IOException {
    synchronized (restTestHarnesses) {
      for (RestTestHarness h : restTestHarnesses) {
        h.close();
      }
    }
  }

  protected static RestTestHarness randomRestTestHarness() {
    return restTestHarnesses.get(random().nextInt(restTestHarnesses.size()));
  }

  protected static RestTestHarness randomRestTestHarness(Random random) {
    return restTestHarnesses.get(random.nextInt(restTestHarnesses.size()));
  }

  protected static void forAllRestTestHarnesses(UnaryOperator<RestTestHarness> op) {
    for (RestTestHarness h : restTestHarnesses) {
      op.apply(h);
    }
  }
  
  public static class AllActive implements CollectionStatePredicate {

    @Override
    public boolean matches(Set<String> liveNodes, DocCollection coll) {
      if (coll == null) return false;
      Collection<Slice> slices = coll.getActiveSlices();
      if (slices == null) return false;
      for (Slice slice : slices) {
        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          if (!replica.getState().equals(State.ACTIVE)) return false;
        }
      }

      return true;
    }
    
  }

  public static RandVal rint = new RandVal() {
    @Override
    public Object val() {
      return random().nextInt();
    }
  };

  public static RandVal rlong = new RandVal() {
    @Override
    public Object val() {
      return random().nextLong();
    }
  };

  public static RandVal rfloat = new RandVal() {
    @Override
    public Object val() {
      return random().nextFloat();
    }
  };

  public static RandVal rdouble = new RandVal() {
    @Override
    public Object val() {
      return random().nextDouble();
    }
  };
  
  public static class RandDate extends RandVal {
    @Override
    public Object val() {
      long v = random().nextLong();
      Date d = new Date(v);
      return d.toInstant().toString();
    }
  }
  
  protected static RandVal[] randVals = new RandVal[]{rint, rfloat, rfloat, rdouble, rdouble, rlong, rlong, rdate, rdate};
}
