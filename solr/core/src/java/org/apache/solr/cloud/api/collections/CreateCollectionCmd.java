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


import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.ALIAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.StrUtils.formatString;
import static org.apache.solr.common.util.Utils.makeMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.TimeSource.NanoTimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandlerApi;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public static final String COLL_PROP_PREFIX = "property.";
  
  private final TimeSource timeSource;
  private final DistribStateManager stateManager;
  private final ZkStateReader zkStateReader;
  private final SolrCloudManager cloudManager;

  private CoreContainer coreContainer;

  public CreateCollectionCmd(CoreContainer cc, SolrCloudManager cloudManager, ZkStateReader zkStateReader) {
    this.coreContainer = cc;
    this.cloudManager = cloudManager;
    this.stateManager = cloudManager.getDistribStateManager();
    this.timeSource = cloudManager.getTimeSource();
    this.zkStateReader = zkStateReader;
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("call(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    if (zkStateReader.aliasesManager != null) { // not a mock ZkStateReader // nocommit alias
      zkStateReader.aliasesManager.update();
    }
    final Aliases aliases = zkStateReader.getAliases();
    final String collectionName = message.getStr(NAME);
    final boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, true);
    final String alias = message.getStr(ALIAS, collectionName);
    log.info("Create collection shards and replicas {}", collectionName);
//    if (clusterState.hasCollection(collectionName)) { // nocommit
//      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection already exists: " + collectionName);
//    }
    if (aliases.hasAlias(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "collection alias already exists: " + collectionName);
    }

    // check if we need to be colocated with another collection
    String withCollection = message.getStr(CollectionAdminParams.WITH_COLLECTION);
    String withCollectionShard = null;
    if (withCollection != null) {
      String realWithCollection = aliases.resolveSimpleAlias(withCollection);
      if (!clusterState.hasCollection(realWithCollection)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "The 'withCollection' does not exist: " + realWithCollection);
      } else  {
        DocCollection collection = clusterState.getCollection(realWithCollection);
        if (collection.getActiveSlices().size() > 1)  {
          throw new SolrException(ErrorCode.BAD_REQUEST, "The `withCollection` must have only one shard, found: " + collection.getActiveSlices().size());
        }
        withCollectionShard = collection.getActiveSlices().iterator().next().getName();
      }
    }

    String configName = getConfigName(collectionName, message);
    if (configName == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No config set found to associate with the collection.");
    }

    boolean isValid = cloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if(!isValid) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }

    String router = message.getStr("router.name", DocRouter.DEFAULT_NAME);

    // get shard names and check replica types
    
    // fail fast if parameters are wrong or incomplete
    List<String> shardNames = populateShardNames(message, router);
    checkReplicaTypes(message);
    
    
    for (String shardName : shardNames) {
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/" + shardName, null, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collectionName + "/leader_elect/" + shardName + "/election", null, CreateMode.PERSISTENT, false);
    }

    AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper = new AtomicReference<>();

    try {

      final String async = message.getStr(ASYNC);

      boolean isLegacyCloud = Overseer.isLegacy(zkStateReader);

      OverseerCollectionMessageHandler.createConfNode(stateManager, configName, collectionName, isLegacyCloud);

      Map<String,String> collectionParams = new HashMap<>();
      Map<String,Object> collectionProps = message.getProperties();
      for (Map.Entry<String, Object> entry : collectionProps.entrySet()) {
        String propName = entry.getKey();
        if (propName.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
          collectionParams.put(propName.substring(ZkController.COLLECTION_PARAM_PREFIX.length()), (String) entry.getValue());
        }
      }

      assert timeSource instanceof NanoTimeSource;

      DocCollection docCollection = buildDocCollection(message, false);
      List<ReplicaPosition> replicaPositions = null;
      try {
        replicaPositions = buildReplicaPositions(cloudManager, clusterState,
            docCollection, message, shardNames, sessionWrapper);
      } catch (Exception e) {
        DW.Exp exp = new DW.Exp("call(ClusterState=" + clusterState + ", ZkNodeProps=" + message + ", NamedList=" + results + ")", e);
        try {
          ZkNodeProps deleteMessage = new ZkNodeProps("name", collectionName);
          new DeleteCollectionCmd(coreContainer, CollectionsHandler.ADMIN_PATH).call(clusterState, deleteMessage, results); // nocommit
          // unwrap the exception
        } catch (Exception e1) {
          DW.propegateInterrupt(e1);
          exp.addSuppressed(e1);
        }
        throw exp;
      } // nocommit example of exception while handling exception

      if (replicaPositions.isEmpty()) {
        if (log.isDebugEnabled()) {
          log.debug("Finished create command for collection: {}", collectionName);
        }
        return;
      }

      final ShardRequestTracker shardRequestTracker = new ShardRequestTracker(CollectionsHandler.ADMIN_PATH, coreContainer, coreContainer.getZkController().getZkStateReader(), async);

      log.info(formatString("Creating SolrCores for new collection {0}, shardNames {1} , message : {2}", collectionName, shardNames, message));
     
      Map<String,ShardRequest> coresToCreate = new LinkedHashMap<>();
      
      ShardHandler shardHandler = ((HttpShardHandlerFactory) coreContainer.getShardHandlerFactory()).getShardHandler(coreContainer.getUpdateShardHandler().getDefaultHttpClient());
      for (ReplicaPosition replicaPosition : replicaPositions) {
        String nodeName = replicaPosition.node;

        if (withCollection != null) {
          log.info("Checking if there is a replica 'withCollection={}' and if not creating one", withCollection);
          // check that we have a replica of `withCollection` on this node and if not, create one
          DocCollection collection = clusterState.getCollection(withCollection);
          List<Replica> replicas = collection.getReplicas(nodeName);
          if (replicas == null || replicas.isEmpty()) {
            log.info("Creating replica to satisfy 'withCollection={}'", withCollection);
            ZkNodeProps props = new ZkNodeProps(
                Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
                ZkStateReader.COLLECTION_PROP, withCollection,
                ZkStateReader.SHARD_ID_PROP, withCollectionShard,
                ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP),
//         /       "shards", message.getStr("shards"),
                "node", nodeName,
                CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.TRUE.toString()); // set to true because we want `withCollection` to be ready after this collection is created
            new AddReplicaCmd(coreContainer, cloudManager, zkStateReader, CollectionsHandler.ADMIN_PATH).call(clusterState, props, results);
            clusterState = zkStateReader.getClusterState(); // refresh
          }
        }

        String coreName = Assign.buildSolrCoreName(cloudManager.getDistribStateManager(),
            docCollection,
            replicaPosition.shard, replicaPosition.type, true);
             log.info(formatString("Creating core {0} as part of shard {1} of collection {2} on {3}"
            , coreName, replicaPosition.shard, collectionName, nodeName));


        String baseUrl = zkStateReader.getBaseUrlForNodeName(nodeName);
        //in the new mode, create the replica in clusterstate prior to creating the core.
        // Otherwise the core creation fails
        
        log.info("Base url for replica={}", baseUrl);
        
        if (!isLegacyCloud) {
          log.info("Sending state update to populate clusterstate with new replica");
          ZkNodeProps props = new ZkNodeProps();
          props.getProperties().putAll(message.getProperties());
          ZkNodeProps addReplicaProps = new ZkNodeProps(
              Overseer.QUEUE_OPERATION, ADDREPLICA.toString(),
              ZkStateReader.COLLECTION_PROP, collectionName,
              ZkStateReader.SHARD_ID_PROP, replicaPosition.shard,
              ZkStateReader.CORE_NAME_PROP, coreName,
              ZkStateReader.STATE_PROP, Replica.State.DOWN.toString(),
              ZkStateReader.BASE_URL_PROP, baseUrl,
              ZkStateReader.NODE_NAME_PROP, nodeName,
              ZkStateReader.REPLICA_TYPE, replicaPosition.type.name(),
              ZkStateReader.NUM_SHARDS_PROP, message.getStr(ZkStateReader.NUM_SHARDS_PROP),
          //    "shards", message.getStr("shards"),
              CommonAdminParams.WAIT_FOR_FINAL_STATE, Boolean.toString(waitForFinalState));
          props.getProperties().putAll(addReplicaProps.getProperties());
          coreContainer.getZkController().sendStateUpdate(props);
        }

        // Need to create new params for each request
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.CREATE.toString());

        params.set(CoreAdminParams.NAME, coreName);
        params.set(COLL_CONF, configName);
        params.set(CoreAdminParams.COLLECTION, collectionName);
        params.set(CoreAdminParams.SHARD, replicaPosition.shard);
        params.set(ZkStateReader.NUM_SHARDS_PROP, shardNames.size());
        params.set(CoreAdminParams.NEW_COLLECTION, "true");
        params.set(CoreAdminParams.REPLICA_TYPE, replicaPosition.type.name());

        if (async != null) {
          String coreAdminAsyncId = async + Math.abs(System.nanoTime());
          params.add(ASYNC, coreAdminAsyncId);
          shardRequestTracker.track(nodeName, coreAdminAsyncId);
        }
        addPropertyParams(message, params);

        ShardRequest sreq = new ShardRequest();
        sreq.nodeName = nodeName;
        params.set("qt", CollectionsHandler.ADMIN_PATH);
        sreq.purpose = 1;
        sreq.shards = new String[]{baseUrl};
        sreq.actualShards = sreq.shards;
        sreq.params = params;

        if (isLegacyCloud) {
          log.info("Submit request to shard for legacyCloud for replica={}", baseUrl);
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        } else {
          coresToCreate.put(coreName, sreq);
        }
      }

      if(!isLegacyCloud) {
        // wait for all replica entries to be created
        Map<String, Replica> replicas = waitToSeeReplicasInState(collectionName, coresToCreate.keySet());
        for (Map.Entry<String, ShardRequest> e : coresToCreate.entrySet()) {
          ShardRequest sreq = e.getValue();
          String coreNodeName = replicas.get(e.getKey()).getName();
          sreq.params.set(CoreAdminParams.CORE_NODE_NAME, coreNodeName);
          log.info("Set the {} for replica {} to {}", CoreAdminParams.CORE_NODE_NAME, replicas.get(e.getKey()), coreNodeName);
          
          log.info("Submit request to shard for for replica={}", sreq.actualShards != null ? Arrays.asList(sreq.actualShards) : "null");
          shardHandler.submit(sreq, sreq.shards[0], sreq.params);
        }
      }

      shardRequestTracker.processResponses(results, shardHandler, false, null, Collections.emptySet());
      boolean failure = results.get("failure") != null && ((SimpleOrderedMap)results.get("failure")).size() > 0;
      if (failure) {
        // Let's cleanup as we hit an exception
        // We shouldn't be passing 'results' here for the cleanup as the response would then contain 'success'
        // element, which may be interpreted by the user as a positive ack
        cleanupCollection(collectionName, new NamedList<Object>());
        log.info("Cleaned up artifacts for failed create collection for [{}]", collectionName);
        throw new SolrException(ErrorCode.BAD_REQUEST, "Underlying core creation failed while creating collection: " + collectionName);
      } else {
        log.debug("Finished create command on all shards for collection: {}", collectionName);

        // Emit a warning about production use of data driven functionality
        boolean defaultConfigSetUsed = message.getStr(COLL_CONF) == null ||
            message.getStr(COLL_CONF).equals(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
        if (defaultConfigSetUsed) {
          results.add("warning", "Using _default configset. Data driven schema functionality"
              + " is enabled by default, which is NOT RECOMMENDED for production use. To turn it off:"
              + " curl http://{host:port}/solr/" + collectionName + "/config -d '{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}'");
        }
      }

      // modify the `withCollection` and store this new collection's name with it
      if (withCollection != null) {
        ZkNodeProps props = new ZkNodeProps(
            Overseer.QUEUE_OPERATION, MODIFYCOLLECTION.toString(),
            ZkStateReader.COLLECTION_PROP, withCollection,
            CollectionAdminParams.COLOCATED_WITH, collectionName);
        coreContainer.getZkController().sendStateUpdate(props);
        try {
          zkStateReader.waitForState(withCollection, 5, TimeUnit.SECONDS, (collectionState) -> collectionName.equals(collectionState.getStr(COLOCATED_WITH)));
        } catch (TimeoutException e) {
          log.warn("Timed out waiting to see the " + COLOCATED_WITH + " property set on collection: " + withCollection);
          // maybe the overseer queue is backed up, we don't want to fail the create request
          // because of this time out, continue
        }
      }

      // create an alias pointing to the new collection, if different from the collectionName
      if (!alias.equals(collectionName)) {
        coreContainer.getZkController().getZkStateReader().aliasesManager.applyModificationAndExportToZk(a -> a.cloneWithCollectionAlias(alias, collectionName));
      }

    } catch (SolrException ex) {
      log.error("Error during collection create", ex);
      throw ex;
    } catch (Exception ex) {
      throw new DW.Exp("Error during collection create(ClusterState=" + clusterState + ", ZkNodeProps=" + message + ", NamedList=" + results + ")", ex);
    } finally {
      if (sessionWrapper.get() != null) sessionWrapper.get().release();
    }

    if (log.isDebugEnabled()) {
      log.debug("call(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  public static List<ReplicaPosition> buildReplicaPositions(SolrCloudManager cloudManager, ClusterState clusterState,
                                                            DocCollection docCollection,
                                                            ZkNodeProps message,
                                                            List<String> shardNames,
                                                            AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper) throws IOException, InterruptedException, Assign.AssignmentException {
    if (log.isDebugEnabled()) {
      log.debug("buildReplicaPositions(SolrCloudManager cloudManager={}, ClusterState clusterState={}, DocCollection docCollection={}, ZkNodeProps message={}, List<String> shardNames={}, AtomicReference<PolicyHelper.SessionWrapper> sessionWrapper={}) - start", cloudManager, clusterState, docCollection, message, shardNames, sessionWrapper);
    }

    final String collectionName = message.getStr(NAME);
    // look at the replication factor and see if it matches reality
    // if it does not, find best nodes to create more cores
    int numTlogReplicas = message.getInt(TLOG_REPLICAS, 0);
    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, numTlogReplicas>0?0:1));
    int numPullReplicas = message.getInt(PULL_REPLICAS, 0);

    int numSlices = shardNames.size();
    int maxShardsPerNode = message.getInt(MAX_SHARDS_PER_NODE, 1);
    if (maxShardsPerNode == -1) maxShardsPerNode = Integer.MAX_VALUE;

    // we need to look at every node and see how many cores it serves
    // add our new cores to existing nodes serving the least number of cores
    // but (for now) require that each core goes on a distinct node.

    List<ReplicaPosition> replicaPositions;
    List<String> nodeList = Assign.getLiveOrLiveAndCreateNodeSetList(clusterState.getLiveNodes(), message, OverseerCollectionMessageHandler.RANDOM);
    if (nodeList.isEmpty()) {
      log.warn("It is unusual to create a collection ("+collectionName+") without cores.");

      replicaPositions = new ArrayList<>();
    } else {
      int totalNumReplicas = numNrtReplicas + numTlogReplicas + numPullReplicas;
      if (totalNumReplicas > nodeList.size()) {
        log.warn("Specified number of replicas of "
            + totalNumReplicas
            + " on collection "
            + collectionName
            + " is higher than the number of Solr instances currently live or live and part of your " + OverseerCollectionMessageHandler.CREATE_NODE_SET + "("
            + nodeList.size()
            + "). It's unusual to run two replica of the same slice on the same Solr-instance.");
      }

      int maxShardsAllowedToCreate = maxShardsPerNode == Integer.MAX_VALUE ?
          Integer.MAX_VALUE :
          maxShardsPerNode * nodeList.size();
      int requestedShardsToCreate = numSlices * totalNumReplicas;
      if (maxShardsAllowedToCreate < requestedShardsToCreate) {
        String msg = "Cannot create collection " + collectionName + ". Value of "
            + MAX_SHARDS_PER_NODE + " is " + maxShardsPerNode
            + ", and the number of nodes currently live or live and part of your "+OverseerCollectionMessageHandler.CREATE_NODE_SET+" is " + nodeList.size()
            + ". This allows a maximum of " + maxShardsAllowedToCreate
            + " to be created. Value of " + OverseerCollectionMessageHandler.NUM_SLICES + " is " + numSlices
            + ", value of " + NRT_REPLICAS + " is " + numNrtReplicas
            + ", value of " + TLOG_REPLICAS + " is " + numTlogReplicas
            + " and value of " + PULL_REPLICAS + " is " + numPullReplicas
            + ". This requires " + requestedShardsToCreate
            + " shards to be created (higher than the allowed number)";
        
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
      }
      Assign.AssignRequest assignRequest = new Assign.AssignRequestBuilder()
          .forCollection(collectionName)
          .forShard(shardNames)
          .assignNrtReplicas(numNrtReplicas)
          .assignTlogReplicas(numTlogReplicas)
          .assignPullReplicas(numPullReplicas)
          .onNodes(nodeList)
          .build();
      Assign.AssignStrategyFactory assignStrategyFactory = new Assign.AssignStrategyFactory(cloudManager);
      Assign.AssignStrategy assignStrategy = assignStrategyFactory.create(clusterState, docCollection);
      replicaPositions = assignStrategy.assign(cloudManager, assignRequest);
      sessionWrapper.set(PolicyHelper.getLastSessionWrapper(true));
    }

    if (log.isDebugEnabled()) {
      log.debug("buildReplicaPositions(SolrCloudManager, ClusterState, DocCollection, ZkNodeProps, List<String>, AtomicReference<PolicyHelper.SessionWrapper>) - end");
    }
    return replicaPositions;
  }

  public static void checkReplicaTypes(ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("checkReplicaTypes(ZkNodeProps message={}) - start", message);
    }

    int numTlogReplicas = message.getInt(TLOG_REPLICAS, 0);
    int numNrtReplicas = message.getInt(NRT_REPLICAS, message.getInt(REPLICATION_FACTOR, numTlogReplicas > 0 ? 0 : 1));

    if (numNrtReplicas + numTlogReplicas <= 0) {
      throw new SolrException(ErrorCode.BAD_REQUEST, NRT_REPLICAS + " + " + TLOG_REPLICAS + " must be greater than 0");
    }

    if (log.isDebugEnabled()) {
      log.debug("checkReplicaTypes(ZkNodeProps) - end");
    }
  }

  public static List<String> populateShardNames(ZkNodeProps message, String router) {
    if (log.isDebugEnabled()) {
      log.debug("populateShardNames(ZkNodeProps message={}, String router={}) - start", message, router);
    }

    List<String> shardNames = new ArrayList<>();
    Integer numSlices = message.getInt(OverseerCollectionMessageHandler.NUM_SLICES, null);
    if (ImplicitDocRouter.NAME.equals(router)) {
      ClusterStateMutator.getShardNames(shardNames, message.getStr("shards", null));
      numSlices = shardNames.size();
    } else {
      if (numSlices == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, OverseerCollectionMessageHandler.NUM_SLICES + " is a required param (when using CompositeId router).");
      }
      if (numSlices <= 0) {
        throw new SolrException(ErrorCode.BAD_REQUEST, OverseerCollectionMessageHandler.NUM_SLICES + " must be > 0");
      }
      ClusterStateMutator.getShardNames(numSlices, shardNames);
    }

    if (log.isDebugEnabled()) {
      log.debug("populateShardNames(ZkNodeProps, String) - end");
    }
    return shardNames;
  }

  String getConfigName(String coll, ZkNodeProps message) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getConfigName(String coll={}, ZkNodeProps message={}) - start", coll, message);
    }

    String configName = message.getStr(COLL_CONF);

    if (configName == null) {
      // if there is only one conf, use that
      List<String> configNames = null;
      try {
        configNames = zkStateReader.getZkClient().getChildren(ZkConfigManager.CONFIGS_ZKNODE, null, true);
        if (configNames.contains(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME)) {
          if (CollectionAdminParams.SYSTEM_COLL.equals(coll)) {
            if (log.isDebugEnabled()) {
              log.debug("getConfigName(String, ZkNodeProps) - end");
            }
            return coll;
          } else {
            String intendedConfigSetName = ConfigSetsHandlerApi.getSuffixedNameForAutoGeneratedConfigSet(coll);
            copyDefaultConfigSetTo(configNames, intendedConfigSetName);

            if (log.isDebugEnabled()) {
              log.debug("getConfigName(String, ZkNodeProps) - end");
            }
            return intendedConfigSetName;
          }
        } else if (configNames != null && configNames.size() == 1) {
          configName = configNames.get(0);
          // no config set named, but there is only 1 - use it
          log.info("Only one config set found in zk - using it:" + configName);
        }
      } catch (KeeperException.NoNodeException e) {
        log.warn("getConfigName(String=" + coll + ", ZkNodeProps=" + message + ") - exception ignored", e);

      }
    }
    String returnString = "".equals(configName) ? null : configName;
    if (log.isDebugEnabled()) {
      log.debug("getConfigName(String, ZkNodeProps) - end");
    }
    return returnString;
  }

  /**
   * Copies the _default configset to the specified configset name (overwrites if pre-existing)
   */
  private void copyDefaultConfigSetTo(List<String> configNames, String targetConfig) {
    if (log.isDebugEnabled()) {
      log.debug("copyDefaultConfigSetTo(List<String> configNames={}, String targetConfig={}) - start", configNames, targetConfig);
    }

    ZkConfigManager configManager = new ZkConfigManager(zkStateReader.getZkClient());

    // if a configset named collection exists, re-use it
    if (configNames.contains(targetConfig)) {
      log.info("There exists a configset by the same name as the collection we're trying to create: " + targetConfig +
          ", re-using it.");
      return;
    }
    // Copy _default into targetConfig
    try {
      configManager.copyConfigDir(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME, targetConfig, new HashSet<>());
    } catch (Exception e) {
     throw new DW.Exp("copyDefaultConfigSetTo(List<String>=" + configNames + ", String=" + targetConfig + ")", e);
    }

    if (log.isDebugEnabled()) {
      log.debug("copyDefaultConfigSetTo(List<String>, String) - end");
    }
  }

  public static void createCollectionZkNode(DistribStateManager stateManager, String collection, Map<String,String> params) {
    if (log.isDebugEnabled()) {
      log.debug("createCollectionZkNode(DistribStateManager stateManager={}, String collection={}, Map<String,String> params={}) - start", stateManager, collection, params);
    }

    String collectionPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    // clean up old terms node
    String termsPath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms";
    try {
      stateManager.removeRecursively(termsPath, true, true);
    } catch (Exception e) {
      throw new DW.Exp("createCollectionZkNode(DistribStateManager=" + stateManager + ", String=" + collection + ", Map<String,String>=" + params + ")", e);
    }
    try {
      log.info("Creating collection in ZooKeeper:" + collection);

      Map<String,Object> collectionProps = new HashMap<>();

      if (params.size() > 0) {
        collectionProps.putAll(params);
        // if the config name wasn't passed in, use the default
        if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP)) {
          // users can create the collection node and conf link ahead of time, or this may return another option
          getConfName(stateManager, collection, collectionPath, collectionProps);
        }

      } else if (System.getProperty("bootstrap_confdir") != null) {
        String defaultConfigName = System
            .getProperty(ZkController.COLLECTION_PARAM_PREFIX + ZkController.CONFIGNAME_PROP, collection);

        // if we are bootstrapping a collection, default the config for
        // a new collection to the collection we are bootstrapping
        log.info("Setting config for collection:" + collection + " to " + defaultConfigName);

        Properties sysProps = System.getProperties();
        for (String sprop : System.getProperties().stringPropertyNames()) {
          if (sprop.startsWith(ZkController.COLLECTION_PARAM_PREFIX)) {
            collectionProps.put(sprop.substring(ZkController.COLLECTION_PARAM_PREFIX.length()),
                sysProps.getProperty(sprop));
          }
        }

        // if the config name wasn't passed in, use the default
        if (!collectionProps.containsKey(ZkController.CONFIGNAME_PROP))
          collectionProps.put(ZkController.CONFIGNAME_PROP, defaultConfigName);

      } else if (Boolean.getBoolean("bootstrap_conf")) {
        // the conf name should should be the collection name of this core
        collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
      } else {
        getConfName(stateManager, collection, collectionPath, collectionProps);
      }

      collectionProps.remove(ZkStateReader.NUM_SHARDS_PROP); // we don't put numShards in the collections properties

      // nocommit make efficient

      ZkNodeProps zkProps = new ZkNodeProps(collectionProps);
      stateManager.makePath(collectionPath, Utils.toJSON(zkProps), CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection
          + "/leader_elect/", null, CreateMode.PERSISTENT, false);
      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/"
          + ZkStateReader.SHARD_LEADERS_ZKNODE, null, CreateMode.PERSISTENT, false);

      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + ZkStateReader.STATE_JSON,
          ZkController.emptyJson, CreateMode.PERSISTENT, false);

      stateManager.makePath(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms", null, CreateMode.PERSISTENT,
          false);

    } catch (Exception e) {
      throw new DW.Exp("createCollectionZkNode(DistribStateManager=" + stateManager + ", String=" + collection + ", Map<String,String>=" + params + ")", e);
    }


    if (log.isDebugEnabled()) {
      log.debug("createCollectionZkNode(DistribStateManager, String, Map<String,String>) - end");
    }
  }
  
  public static DocCollection buildDocCollection(ZkNodeProps message, boolean withDocRouter) {
    withDocRouter = true;
    String cName = message.getStr(NAME);
    DocRouter router = null;
    Map<String,Object> routerSpec = null;
    if (withDocRouter) {
      routerSpec = DocRouter.getRouterSpec(message);
      String routerName = routerSpec.get(NAME) == null ? DocRouter.DEFAULT_NAME : (String) routerSpec.get(NAME);
      router = DocRouter.getDocRouter(routerName);
    }
    Object messageShardsObj = message.get("shards");

    Map<String,Slice> slices;
    if (messageShardsObj instanceof Map) { // we are being explicitly told the slice data (e.g. coll restore)
      slices = Slice.loadAllFromMap((Map<String,Object>) messageShardsObj);
    } else {
      List<String> shardNames = new ArrayList<>();
      if (withDocRouter) {
        if (router instanceof ImplicitDocRouter) {
          getShardNames(shardNames, message.getStr("shards", DocRouter.DEFAULT_NAME));
        } else {
          int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, -1);
          if (numShards < 1)
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "numShards is a required parameter for 'compositeId' router {}" + message);
          getShardNames(numShards, shardNames);
        }
      }

      List<DocRouter.Range> ranges = null;
      if (withDocRouter) {
        ranges = router.partitionRange(shardNames.size(), router.fullRange());// maybe null
      }
      slices = new LinkedHashMap<>();
      for (int i = 0; i < shardNames.size(); i++) {
        String sliceName = shardNames.get(i);

        Map<String,Object> sliceProps = new LinkedHashMap<>(1);

        if (withDocRouter) {
          sliceProps.put(Slice.RANGE, ranges == null ? null : ranges.get(i));
        }

        slices.put(sliceName, new Slice(sliceName, null, sliceProps));

      }
    }

    Map<String,Object> collectionProps = new HashMap<>();

    for (Map.Entry<String,Object> e : OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.entrySet()) {
      Object val = message.get(e.getKey());
      if (val == null) {
        val = OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.get(e.getKey());
      }
      if (val != null) collectionProps.put(e.getKey(), val);
    }
    if (withDocRouter) {
      collectionProps.put(DocCollection.DOC_ROUTER, routerSpec);
    }
    if (withDocRouter) {

      if (message.getStr("fromApi") == null) {
        collectionProps.put("autoCreated", "true");
      }
    }

    // TODO default to 2; but need to debug why BasicDistributedZk2Test fails early on
    String znode = message.getInt(DocCollection.STATE_FORMAT, 1) == 1 ? null
        : ZkStateReader.getCollectionPath(cName);

    DocCollection newCollection = new DocCollection(cName,
        slices, collectionProps, router, -1, znode);

    return newCollection;
  }
  
  public static void getShardNames(Integer numShards, List<String> shardNames) {
    if (numShards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "numShards" + " is a required param");
    for (int i = 0; i < numShards; i++) {
      final String sliceName = "shard" + (i + 1);
      shardNames.add(sliceName);
    }
  }
  
  public static void getShardNames(List<String> shardNames, String shards) {
    if (shards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
    for (String s : shards.split(",")) {
      if (s == null || s.trim().isEmpty()) continue;
      shardNames.add(s.trim());
    }
    if (shardNames.isEmpty())
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
  }

  private static void getConfName(DistribStateManager stateManager, String collection, String collectionPath, Map<String,Object> collectionProps) throws IOException,
      KeeperException, InterruptedException {
    // check for configName
    log.debug("Looking for collection configName");
    if (collectionProps.containsKey("configName")) {
      log.info("configName was passed as a param {}", collectionProps.get("configName"));
      return;
    }

    List<String> configNames = null;
    int retry = 1;
    int retryLimt = 6;
    for (; retry < retryLimt; retry++) {
      if (stateManager.hasData(collectionPath)) {
        VersionedData data = stateManager.getData(collectionPath);
        ZkNodeProps cProps = ZkNodeProps.load(data.getData());
        if (cProps.containsKey(ZkController.CONFIGNAME_PROP)) {
          break;
        }
      }

      try {
        configNames = stateManager.listData(ZkConfigManager.CONFIGS_ZKNODE);
      } catch (NoSuchElementException | NoNodeException e) {
        log.warn("getConfName(DistribStateManager=" + stateManager + ", String=" + collection + ", String=" + collectionPath + ", Map<String,Object>=" + collectionProps + ") - exception ignored", e);

        // just keep trying
      }

      // check if there's a config set with the same name as the collection
      if (configNames != null && configNames.contains(collection)) {
        log.info(
            "Could not find explicit collection configName, but found config name matching collection name - using that set.");
        collectionProps.put(ZkController.CONFIGNAME_PROP, collection);
        break;
      }
      // if _default exists, use that
      if (configNames != null && configNames.contains(ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME)) {
        log.info(
            "Could not find explicit collection configName, but found _default config set - using that set.");
        collectionProps.put(ZkController.CONFIGNAME_PROP, ConfigSetsHandlerApi.DEFAULT_CONFIGSET_NAME);
        break;
      }
      // if there is only one conf, use that
      if (configNames != null && configNames.size() == 1) {
        // no config set named, but there is only 1 - use it
        log.info("Only one config set found in zk - using it:" + configNames.get(0));
        collectionProps.put(ZkController.CONFIGNAME_PROP, configNames.get(0));
        break;
      }

      log.info("Could not find collection configName - pausing for 3 seconds and trying again - try: " + retry);
      Thread.sleep(3000);
    }
    if (retry == retryLimt) {
      log.error("Could not find configName for collection " + collection);
      throw new ZooKeeperException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not find configName for collection " + collection + " found:" + configNames);
    }

    if (log.isDebugEnabled()) {
      log.debug("getConfName(DistribStateManager, String, String, Map<String,Object>) - end");
    }
  }
  
  void addPropertyParams(ZkNodeProps message, ModifiableSolrParams params) {
    if (log.isDebugEnabled()) {
      log.debug("addPropertyParams(ZkNodeProps message={}, ModifiableSolrParams params={}) - start", message, params);
    }

    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        params.set(key, message.getStr(key));
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("addPropertyParams(ZkNodeProps, ModifiableSolrParams) - end");
    }
  }

  void addPropertyParams(ZkNodeProps message, Map<String, Object> map) {
    if (log.isDebugEnabled()) {
      log.debug("addPropertyParams(ZkNodeProps message={}, Map<String,Object> map={}) - start", message, map);
    }

    // Now add the property.key=value pairs
    for (String key : message.keySet()) {
      if (key.startsWith(COLL_PROP_PREFIX)) {
        map.put(key, message.getStr(key));
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("addPropertyParams(ZkNodeProps, Map<String,Object>) - end");
    }
  }

  void cleanupCollection(String collectionName, NamedList results) throws Exception {
    log.error("Cleaning up collection [" + collectionName + "]." );
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    new DeleteCollectionCmd(coreContainer, CollectionsHandler.ADMIN_PATH).call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);

    if (log.isDebugEnabled()) {
      log.debug("cleanupCollection(String, NamedList) - end");
    }
  }
  
  // nocommit
  
  // this should be picking up the coreNodeName for a replica
  Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    AtomicReference<Map<String, Replica>> result = new AtomicReference<>();
    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      zkStateReader.waitForState(collectionName, 15, TimeUnit.SECONDS, (n, c) -> { // nocommit - univeral config wait
        if (c == null)
          return false;
        Map<String, Replica> r = new HashMap<>();
        for (String coreName : coreNames) {
          if (r.containsKey(coreName)) continue;
          for (Slice slice : c.getSlices()) {
            for (Replica replica : slice.getReplicas()) {
              if (coreName.equals(replica.getStr(ZkStateReader.CORE_NAME_PROP))) {
                r.put(coreName, replica);
                break;
              }
            }
          }
        }
        
        if (r.size() == coreNames.size()) {
          result.set(r);
          return true;
        } else {
          errorMessage.set("Timed out waiting to see all replicas: " + coreNames + " in cluster state. Last state: " + c);
          return false;
        }

      });
    } catch (TimeoutException | InterruptedException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state.";
      throw new SolrException(ErrorCode.SERVER_ERROR, error);
    }
    return result.get();
  }
}
