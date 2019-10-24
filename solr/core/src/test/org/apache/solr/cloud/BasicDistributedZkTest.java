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

import static org.apache.solr.SolrTestCaseJ4.ignoreException;
import static org.apache.solr.SolrTestCaseJ4.map;
import static org.apache.solr.SolrTestCaseJ4.unIgnoreException;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.IOUtils;
import org.apache.solr.JSONTestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.StreamingUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.Group;
import org.apache.solr.client.solrj.response.GroupCommand;
import org.apache.solr.client.solrj.response.GroupResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.RetryUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.solr.util.TestInjection;
import org.apache.solr.util.TestInjection.Hook;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This test simply does a bunch of basic things in solrcloud mode and asserts things
 * work as expected.
 */
@SuppressSSL(bugUrl = "https://issues.apache.org/jira/browse/SOLR-5776")
public class BasicDistributedZkTest extends SolrCloudBridgeTestCase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String DEFAULT_COLLECTION = "collection1";

  private final boolean onlyLeaderIndexes = random().nextBoolean();

  private Map<String,List<SolrClient>> otherCollectionClients = new HashMap<>();

  private String oneInstanceCollection = "oneInstanceCollection";
  private String oneInstanceCollection2 = "oneInstanceCollection2";
  
  private AtomicInteger nodeCounter = new AtomicInteger();
  
  protected ExecutorService executor = new ExecutorUtil.MDCAwareThreadPoolExecutor(
      4,
      Integer.MAX_VALUE,
      15, TimeUnit.SECONDS, // terminate idle threads after 15 sec
      new SynchronousQueue<>(),  // directly hand off tasks
      new DefaultSolrThreadFactory("BaseDistributedSearchTestCase"),
      false
  );
  
  CompletionService<Object> completionService;
  Set<Future<Object>> pending;
  
  private static Hook newSearcherHook = new Hook() {
    volatile CountDownLatch latch;
    AtomicReference<String> collection = new AtomicReference<>();

    @Override
    public void newSearcher(String collectionName) {
      String c = collection.get();
      if (c  != null && c.equals(collectionName)) {
        log.info("Hook detected newSearcher");
        try {
          latch.countDown();
        } catch (NullPointerException e) {

        }
      }
    }
  
    public void waitForSearcher(int timeoutms, boolean failOnTimeout) throws InterruptedException {

      boolean timeout = !latch.await(timeoutms, TimeUnit.MILLISECONDS);
      if (timeout && failOnTimeout) {
        fail("timed out waiting for new searcher event " + latch.getCount());
      }
    }

    @Override
    public void insertHook(String collection, int cnt) {
      latch = new CountDownLatch(cnt);
      this.collection.set(collection);
    }
  
  };
  
  public BasicDistributedZkTest() {
    // we need DVs on point fields to compute stats & facets
    if (Boolean.getBoolean(SolrTestCaseJ4.NUMERIC_POINTS_SYSPROP)) System.setProperty(SolrTestCaseJ4.NUMERIC_DOCVALUES_SYSPROP,"true");
    
    //sliceCount = 2;
    completionService = new ExecutorCompletionService<>(executor);
    pending = new HashSet<>();
    
    //fixShardCount(TEST_NIGHTLY ? 3 : 2);
  }
  
  @BeforeClass
  public static void beforeBDZKTClass() {
    enableMetricsForNonNightly();
    TestInjection.newSearcherHook(newSearcherHook);
  }

  protected boolean useTlogReplicas() {
    return false; // TODO: tlog replicas makes commits take way to long due to what is likely a bug and it's TestInjection use
  }

  protected void setDistributedParams(ModifiableSolrParams params) {

    if (random().nextBoolean()) {
      // don't set shards, let that be figured out from the cloud state
    } else {
      // use shard ids rather than physical locations
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < getShardCount(); i++) {
        if (i > 0)
          sb.append(',');
        sb.append("shard" + (i + 3));
      }
      params.set("shards", sb.toString());
    }
  }

  @Test
  // commented out on: 17-Feb-2019   @BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // annotated on: 24-Dec-2018
  public void basicSolrCloudTest() throws Exception {
    // setLoggingLevel(null);


   // queryAndCompareShards(params("q", "*:*", "distrib", "false", "sanity_check", "is_empty"));

    if (TEST_NIGHTLY) {
      // ask every individual replica of every shard to update+commit the same doc id
      // with an incrementing counter on each update+commit
      int foo_i_counter = 0;

        foo_i_counter++;
        indexDoc(cloudClient, params("commit", "true"), // SOLR-4923
            sdoc(id, 1, i1, 100, tlong, 100, "foo_i", foo_i_counter));
        // after every update+commit, check all the shards consistency
//        queryAndCompareShards(params("q", "id:1", "distrib", "false",
//            "sanity_check", "non_distrib_id_1_lookup"));
//        queryAndCompareShards(params("q", "id:1",
//            "sanity_check", "distrib_id_1_lookup"));
     
    }

    indexr(id,1, i1, 100, tlong, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d, tsort, "now is the time for all good men");
    indexr(id, 2, i1, 50, tlong, 50, t1, "to come to the aid of their country."
        , tsort, "to come to the aid of their country.");
    indexr(id, 3, i1, 2, tlong, 2, t1, "how now brown cow", tsort, "how now brown cow");
    indexr(id, 4, i1, -100, tlong, 101, t1, "the quick fox jumped over the lazy dog"
        , tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 5, i1, 500, tlong, 500, t1, "the quick fox jumped way over the lazy dog"
        , tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 6, i1, -600, tlong, 600, t1, "humpty dumpy sat on a wall", tsort, "the quick fox jumped over the lazy dog");
    indexr(id, 7, i1, 123, tlong, 123, t1, "humpty dumpy had a great fall", tsort, "the quick fox jumped over the lazy dog");
    indexr(id,8, i1, 876, tlong, 876,t1,"all the kings horses and all the kings men",tsort,"all the kings horses and all the kings men");
    indexr(id, 9, i1, 7, tlong, 7, t1, "couldn't put humpty together again", tsort, "the quick fox jumped over the lazy dog");
    indexr(id,10, i1, 4321, tlong, 4321,t1,"this too shall pass",tsort,"this too shall pass");
    indexr(id,11, i1, -987, tlong, 987,t1,"An eye for eye only ends up making the whole world blind."
        ,tsort,"An eye for eye only ends up making the whole world blind.");
    indexr(id,12, i1, 379, tlong, 379,t1,"Great works are performed, not by strength, but by perseverance.",
        tsort,"Great works are performed, not by strength, but by perseverance.");
    indexr(id,13, i1, 232, tlong, 232,t1,"no eggs on wall, lesson learned", oddField, "odd man out",
        tsort,"no eggs on wall, lesson learned");

    indexr(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    indexr(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    indexr(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    indexr(id, 17, "SubjectTerms_mfacet", vals);

    for (int i=100; i<150; i++) {
      indexr(id, i);      
    }

    commit();

    if (TEST_NIGHTLY) testTokenizedGrouping();
    testSortableTextFaceting();
    if (TEST_NIGHTLY) testSortableTextSorting();
    testSortableTextGrouping();

//    queryAndCompareShards(params("q", "*:*", 
//                                 "sort", "id desc",
//                                 "distrib", "false", 
//                                 "sanity_check", "is_empty"));

    // random value sort
    for (String f : fieldNames) {
      query(false, new String[] {"q","*:*", "sort",f+" desc"});
      query(false, new String[] {"q","*:*", "sort",f+" asc"});
    }

    // these queries should be exactly ordered and scores should exactly match
    query(false, new String[] {"q","*:*", "sort",i1+" desc"});
    query(false, new String[] {"q","*:*", "sort",i1+" asc"});
    query(false, new String[] {"q","*:*", "sort",i1+" desc", "fl","*,score"});
    query(false, new String[] {"q","*:*", "sort","n_tl1 asc", "fl","*,score"}); 
    query(false, new String[] {"q","*:*", "sort","n_tl1 desc"});
    handle.put("maxScore", SKIPVAL);
    query(false, new String[] {"q","{!func}"+i1});// does not expect maxScore. So if it comes ,ignore it. JavaBinCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query(false, new String[] {"q","{!func}"+i1, "fl","*,score"});  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query(false, new String[] {"q","quick"});
    query(false, new String[] {"q","all","fl","id","start","0"});
    query(false, new String[] {"q","all","fl","foofoofoo","start","0"});  // no fields in returned docs
    query(false, new String[] {"q","all","fl","id","start","100"});

    handle.put("score", SKIPVAL);
    query(false, new String[] {"q","quick","fl","*,score"});
    query(false, new String[] {"q","all","fl","*,score","start","1"});
    query(false, new String[] {"q","all","fl","*,score","start","100"});

    query(false, new String[] {"q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1});

    query(false, new String[] {"q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1});

    query(false, new String[] {"q","matchesnothing","fl","*,score"});  

    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count", "facet.mincount",2});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index", "facet.mincount",2});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1,"facet.limit",1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.offset",1});
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.mincount",2});

    // test faceting multiple things at once
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1});

    // test filter tagging, facet exclusion, and naming (multi-select facet support)
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id_i1:[1 TO 7]", "fq","{!tag=b}id_i1:[3 TO 9]"}
    );
    query(false, new Object[] {"q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", "10", "facet.mincount", "1"});

    // test field that is valid in schema but missing in all shards
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2});
    // test field that is valid in schema and missing in some shards
    query(false, new Object[] {"q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2});

    query(false, new Object[] {"q","*:*", "sort",i1+" desc", "stats", "true", "stats.field", i1});

    /*** TODO: the failure may come back in "exception"
    try {
      // test error produced for field that is invalid for schema
      query("q","*:*", "rows",100, "facet","true", "facet.field",invalidField, "facet.mincount",2);
      TestCase.fail("SolrServerException expected for invalid field that is not in schema");
    } catch (SolrServerException ex) {
      // expected
    }
    ***/

    // Try to get better coverage for refinement queries by turning off over requesting.
    // This makes it much more likely that we may not get the top facet values and hence
    // we turn of that checking.
   // handle.put("facet_fields", SKIPVAL);    
    query(false, new Object[] {"q","*:*", "rows",0, "facet","true", "facet.field",t1,"facet.limit",5, "facet.shard.limit",5});
    // check a complex key name
    query(false, new Object[] {"q","*:*", "rows",0, "facet","true", "facet.field","{!key='a b/c \\' \\} foo'}"+t1,"facet.limit",5, "facet.shard.limit",5});
   // handle.remove("facet_fields");



    // test debugging
//    handle.put("explain", SKIPVAL);
//    handle.put("debug", UNORDERED);
//    handle.put("time", SKIPVAL);
//    handle.put("track", SKIP);
    query(false, new Object[] {"q","now their fox sat had put","fl","*,score",CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG_QUERY, "true"});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.TIMING});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.RESULTS});
    query(false, new Object[] {"q", "id_i1:[1 TO 5]", CommonParams.DEBUG, CommonParams.QUERY});

    // try add commitWithin
    long before = cloudClient.query(new SolrQuery("*:*")).getResults().getNumFound();
    System.out.println("before:" + before);

    SolrClient client = getClient(0);
    assertEquals("unexpected pre-commitWithin document count on node: " + ((HttpSolrClient)client).getBaseURL() + "/" + DEFAULT_COLLECTION, before, client.query(new SolrQuery("*:*")).getResults().getNumFound());

    newSearcherHook.insertHook(DEFAULT_COLLECTION, 1);
    
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("commitWithin", 1);
    add(cloudClient, params , getDoc("id", 300), getDoc("id", 301));

    newSearcherHook.waitForSearcher(5000, false);
    
    ClusterState clusterState = cloudClient.getZkStateReader().getClusterState();
    DocCollection dColl = clusterState.getCollection(DEFAULT_COLLECTION);

    assertSliceCounts("should have found 2 docs, 300 and 301", before + 2, DEFAULT_COLLECTION);

    // try deleteById commitWithin
    newSearcherHook.insertHook(DEFAULT_COLLECTION, 1);
    UpdateRequest deleteByIdReq = new UpdateRequest();
    deleteByIdReq.deleteById("300");
    deleteByIdReq.setCommitWithin(10);
    deleteByIdReq.process(cloudClient);
    
    newSearcherHook.waitForSearcher( 5000, false);

    assertSliceCounts("deleteById commitWithin did not work", before + 1, DEFAULT_COLLECTION);
    
    // try deleteByQuery commitWithin
    newSearcherHook.insertHook(DEFAULT_COLLECTION, 1);
    UpdateRequest deleteByQueryReq = new UpdateRequest();
    deleteByQueryReq.deleteByQuery("id:301");
    deleteByQueryReq.setCommitWithin(1);
    deleteByQueryReq.process(cloudClient);

    newSearcherHook.waitForSearcher(5000, false);
    
    assertSliceCounts("deleteByQuery commitWithin did not work", before, DEFAULT_COLLECTION);
    

    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    // query("q","matchesnothing","fl","*,score", "debugQuery", "true");

  }

  private void testSortableTextFaceting() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.addFacetField(tsort);
    query.setFacetMissing(false);
    QueryResponse resp = queryServer(query);
    List<FacetField> ffs = resp.getFacetFields();
    for (FacetField ff : ffs) {
      if (ff.getName().equals(tsort) == false) continue;
      for (FacetField.Count count : ff.getValues()) {
        long num = count.getCount();
        switch (count.getName()) {
          case "all the kings horses and all the kings men":
          case "An eye for eye only ends up making the whole world blind.":
          case "Great works are performed, not by strength, but by perseverance.":
          case "how now brown cow":
          case "no eggs on wall, lesson learned":
          case "now is the time for all good men":
          case "this too shall pass":
          case "to come to the aid of their country.":
            assertEquals("Should have exactly one facet count for field " + ff.getName(), 1, num);
            break;
          case "the quick fox jumped over the lazy dog":
            assertEquals("Should have 5 docs for the lazy dog", 5, num);
            break;
          default:
            fail("No case for facet '" + ff.getName() + "'");

        }
      }
    }
  }

  private void testSortableTextSorting() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.addSort(tsort, SolrQuery.ORDER.desc);
    query.addField("*");
    query.addField("eoe_sortable");
    query.addField(tsort);
    QueryResponse resp = queryServer(query);

    SolrDocumentList docs = resp.getResults();

    String title = docs.get(0).getFieldValue(tsort).toString();
    for (SolrDocument doc : docs) {
      assertTrue("Docs should be back in sorted order, descending", title.compareTo(doc.getFieldValue(tsort).toString()) >= 0);
      title = doc.getFieldValue(tsort).toString();
    }
  }

  private void testSortableTextGrouping() throws Exception {
    SolrQuery query = new SolrQuery("*:*");
    query.add("group", "true");
    query.add("group.field", tsort);
    QueryResponse resp = queryServer(query);
    GroupResponse groupResp = resp.getGroupResponse();
    List<GroupCommand> grpCmds = groupResp.getValues();
    for (GroupCommand grpCmd : grpCmds) {
      if (grpCmd.getName().equals(tsort) == false) continue;
      for (Group grp : grpCmd.getValues()) {
        long count = grp.getResult().getNumFound();
        if (grp.getGroupValue() == null) continue; // Don't count the groups without an entry as the numnber is variable
        switch (grp.getGroupValue()) {
          case "all the kings horses and all the kings men":
          case "An eye for eye only ends up making the whole world blind.":
          case "Great works are performed, not by strength, but by perseverance.":
          case "how now brown cow":
          case "no eggs on wall, lesson learned":
          case "now is the time for all good men":
          case "this too shall pass":
          case "to come to the aid of their country.":
            assertEquals("Should have exactly one facet count for field " + grpCmd.getName(), 1, count);
            break;
          case "the quick fox jumped over the lazy dog":
            assertEquals("Should have 5 docs for the lazy dog", 5, count);
            break;
          default:
            fail("No case for facet '" + grpCmd.getName() + "'");

        }
      }
    }
  }

  private void testTokenizedGrouping() throws Exception {
    SolrException ex = expectThrows(SolrException.class, () -> {
      query(false, new String[]{"q", "*:*", "group", "true", "group.field", t1});
    });
    assertTrue("Expected error from server that SortableTextFields are required", ex.getMessage().contains("Sorting on a tokenized field that is not a SortableTextField is not supported in cloud mode"));
  }

  private void assertSliceCounts(String msg, long expected, String collection) throws Exception {
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    cloudClient.getZkStateReader().waitForState(collection, 3000, TimeUnit.SECONDS, (n,c) -> checkSlicesSameCounts(c) == expected);
    assertEquals(msg, expected, checkSlicesSameCounts(zkStateReader.getClusterState().getCollection(collection)));
  }

  // Insure that counts are the same for all replicas in each shard
  // Return the total doc count for the query.
  private long checkSlicesSameCounts(DocCollection dColl) {
    long docTotal = 0; // total number of documents found counting only one replica per slice.
    for (Slice slice : dColl.getActiveSlices()) {
      long sliceDocCount = -1;
      for (Replica rep : slice.getReplicas()) {
        try (HttpSolrClient one = getHttpSolrClient(rep.getCoreUrl())) {
          SolrQuery query = new SolrQuery("*:*");
          query.setDistrib(false);
          QueryResponse resp = one.query(query);
          long hits = resp.getResults().getNumFound();
          if (sliceDocCount == -1) {
            sliceDocCount = hits;
            docTotal += hits;
          } else {
            if (hits != sliceDocCount) {
              return -1;
            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (SolrServerException e) {
          throw new RuntimeException(e);
        }
      }
    }
    System.out.println("total:" + docTotal);
    return docTotal;
  }

  protected String getBaseUrl(SolrClient client) {
    String url2 = ((HttpSolrClient) client).getBaseURL()
        .substring(
            0,
            ((HttpSolrClient) client).getBaseURL().length()
                - DEFAULT_COLLECTION.length() -1);
    return url2;
  }
  
  @Test
  public void testUpdateProcessorsRunOnlyOnce() throws Exception {
    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-explicit");
    testUpdateProcessorsRunOnlyOnce("distrib-dup-test-chain-implicit");
  }
  
  /**
   * Expects a RegexReplaceProcessorFactories in the chain which will
   * "double up" the values in two (stored) string fields.
   * <p>
   * If the values are "double-doubled" or "not-doubled" then we know 
   * the processor was not run the appropriate number of times
   * </p>
   */
 
  private void testUpdateProcessorsRunOnlyOnce(final String chain) throws Exception {
    final String fieldA = "regex_dup_A_s";
    final String fieldB = "regex_dup_B_s";
    final String val = "x";
    final String expected = "x_x";
    final ModifiableSolrParams updateParams = new ModifiableSolrParams();
    updateParams.add(UpdateParams.UPDATE_CHAIN, chain);
    
    final int numLoops = atLeast(TEST_NIGHTLY ? 50 : 15);
    
    for (int i = 1; i < numLoops; i++) {
      // add doc to random client
  
      SolrInputDocument doc = new SolrInputDocument();
      addFields(doc, id, i, fieldA, val, fieldB, val);
      UpdateResponse ures = add(cloudClient, updateParams, doc);
      assertEquals(chain + ": update failed", 0, ures.getStatus());
      ures = cloudClient.commit();
      assertEquals(chain + ": commit failed", 0, ures.getStatus());
    }

    // query for each doc, and check both fields to ensure the value is correct
    for (int i = 1; i < numLoops; i++) {
      final String query = id + ":" + i;
      QueryResponse qres = queryServer(new SolrQuery(query));
      assertEquals(chain + ": query failed: " + query, 
                   0, qres.getStatus());
      assertEquals(chain + ": didn't find correct # docs with query: " + query,
                   1, qres.getResults().getNumFound());
      SolrDocument doc = qres.getResults().get(0);

      for (String field : new String[] {fieldA, fieldB}) { 
        assertEquals(chain + ": doc#" + i+ " has wrong value for " + field,
                     expected, doc.getFirstValue(field));
      }
    }

  }

  protected static void addFields(SolrInputDocument doc, Object... fields) {
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
  }
  
  // cloud level test mainly needed just to make sure that versions and errors are propagated correctly
  @Test
  @Nightly
  public void doOptimisticLockingAndUpdating() throws Exception {
    log.info("### STARTING doOptimisticLockingAndUpdating");
    
    final SolrInputDocument sd =  sdoc("id", 1000, "_version_", -1);
    indexDoc(sd);

    ignoreException("version conflict");
    SolrException e = expectThrows(SolrException.class, () -> cloudClient.add(sd));
    assertEquals(409, e.code());
   
    unIgnoreException("version conflict");

    // TODO: test deletes.  SolrJ needs a good way to pass version for delete...

    final SolrInputDocument sd2 =  sdoc("id", 1000, "foo_i",5);
    cloudClient.add(sd2);

    List<Integer> expected = new ArrayList<>();
    int val = 0;

      val += 10;
      cloudClient.add(sdoc("id", 1000, "val_i", map("add",val), "foo_i",val));
      expected.add(val);
    

    QueryRequest qr = new QueryRequest(params("qt", "/get", "id","1000"));

      val += 10;
      NamedList rsp = cloudClient.request(qr);
      String match = JSONTestUtil.matchObj("/val_i", rsp.get("doc"), expected);
      if (match != null) throw new RuntimeException(match);
    
  }

  @Test
  @AwaitsFix(bugUrl = "this does not work if run before the other tests")
  public void testNumberOfCommitsWithCommitAfterAdd() throws Exception {
    log.info("### STARTING testNumberOfCommitsWithCommitAfterAdd");
    long startCommits = getNumCommits((HttpSolrClient) getClient(0));


    NamedList<Object> result = cloudClient.request(
        new StreamingUpdateRequest("/update",
            getFile("books_numeric_ids.csv"), "application/csv")
            .setCommitWithin(900000)
            .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true));
    
    long endCommits = getNumCommits((HttpSolrClient) getClient(0));
    
    RetryUtil.retryUntil("", 15, 100, TimeUnit.MILLISECONDS, () -> {
      try {
        return (startCommits + 1L == getNumCommits((HttpSolrClient) getClient(0)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    getNumCommits((HttpSolrClient) getClient(0));
    
    assertEquals(startCommits + 1L, endCommits);
  }

  private Long getNumCommits(HttpSolrClient sourceClient) throws
      SolrServerException, IOException {
    // construct the /admin/metrics URL
    URL url = new URL(sourceClient.getBaseURL());
    String path = url.getPath().substring(1);
    String[] elements = path.split("/");
    String collection = elements[elements.length - 1];
    String urlString = url.toString();
    urlString = urlString.substring(0, urlString.length() - collection.length() - 1);
    try (HttpSolrClient client = getHttpSolrClient(urlString, 15000, 60000)) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      //params.set("qt", "/admin/metrics?prefix=UPDATE.updateHandler&registry=solr.core." + collection);
      params.set("qt", "/admin/metrics");
      params.set("prefix", "UPDATE.updateHandler");
      params.set("registry", "solr.core." + collection);
      // use generic request to avoid extra processing of queries
      QueryRequest req = new QueryRequest(params);
      NamedList<Object> resp = client.request(req);
      
      System.out.println("RESP:" + resp);
      
      NamedList metrics = (NamedList) resp.get("metrics");
      if (metrics.size() == 0) {
        return 0L;
      }

      NamedList uhandlerCat = (NamedList) metrics.getVal(0);
      Map<String,Object> commits = (Map<String,Object>) uhandlerCat.get("UPDATE.updateHandler.commits");
      return (Long) commits.get("count");
    }
  }

  @Test
  public void testANewCollectionInOneInstanceWithManualShardAssignement() throws Exception {
    log.info("### STARTING testANewCollectionInOneInstanceWithManualShardAssignement");
    assertEquals(0, CollectionAdminRequest.createCollection(oneInstanceCollection2, "conf1", 2, 2)
        .setCreateNodeSet("")
        .setMaxShardsPerNode(4)
        .process(cloudClient).getStatus());

    List<SolrClient> collectionClients = new ArrayList<>();
    for (int i = 0; i < (TEST_NIGHTLY ? 4 : 2); i++) {
      CollectionAdminResponse resp = CollectionAdminRequest
          .addReplicaToShard(oneInstanceCollection2, "shard" + ((i%2)+1))
          .setNode(cluster.getJettySolrRunner(0).getNodeName())
          .process(cloudClient);
      for (String coreName : resp.getCollectionCoresStatus().keySet()) {
        collectionClients.add(createNewSolrClient(coreName, cluster.getJettySolrRunner(0).getBaseUrl().toString()));
      }
    }
    
    SolrClient client1 = collectionClients.get(0);
    
    cluster.waitForActiveCollection(oneInstanceCollection2, 2, TEST_NIGHTLY ? 4 : 2);

    client1.add(getDoc(id, "1")); 
    client1.add(getDoc(id, "2")); 
    client1.add(getDoc(id, "3")); 
    
    client1.commit();
    SolrQuery query = new SolrQuery("*:*");
    query.set("distrib", false);
    
    query.set("collection", oneInstanceCollection2);
    query.set("distrib", true);
    long allDocs = cloudClient.query(query).getResults().getNumFound();
    

    assertEquals(3, allDocs);
    
    // we added a role of none on these creates - check for it
    ZkStateReader zkStateReader = cloudClient.getZkStateReader();
    zkStateReader.forceUpdateCollection(oneInstanceCollection2);
    Map<String,Slice> slices = zkStateReader.getClusterState().getCollection(oneInstanceCollection2).getSlicesMap();
    assertNotNull(slices);

    IOUtils.close(collectionClients);

  }
  
  protected void indexDoc(String collection, SolrInputDocument doc) throws IOException, SolrServerException {
    List<SolrClient> clients = otherCollectionClients.get(collection);
    int which = (doc.getField(id).toString().hashCode() & 0x7fffffff) % clients.size();
    SolrClient client = clients.get(which);
    client.add(doc);
  }
  
  protected SolrClient createNewSolrClient(String collection, String baseUrl) {
    try {
      // setup the server...
      HttpSolrClient client = getHttpSolrClient(baseUrl + "/" + collection);

      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  protected SolrClient createNewSolrClient(String collection, String baseUrl, int connectionTimeoutMillis, int socketTimeoutMillis) {
    try {
      // setup the server...
      HttpSolrClient client = getHttpSolrClient(baseUrl + "/" + collection, connectionTimeoutMillis, socketTimeoutMillis);

      return client;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  @Override
  protected QueryResponse queryServer(ModifiableSolrParams params) throws Exception {

    if (random().nextBoolean())
      return super.queryServer(params);

    if (random().nextBoolean())
      params.set("collection",DEFAULT_COLLECTION);

    QueryResponse rsp;
    
    if (controlClient != null) {
      rsp = controlClient.query(params);
    } else {
      rsp = cloudClient.query(params);;
    }
 
    return rsp;
  }
  
  @After
  public void distribTearDown() throws Exception {
    if (otherCollectionClients != null) {
      for (List<SolrClient> clientList : otherCollectionClients.values()) {
        IOUtils.close(clientList);
      }
    }
    otherCollectionClients = null;
    ExecutorUtil.shutdownAndAwaitTermination(executor);
  }
}
