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

import static org.apache.solr.client.solrj.cloud.autoscaling.Policy.POLICY;
import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.ZkStateReader.BASE_URL_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ALIASPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BACKUP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESNAPSHOT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MAINTAINROUTEDALIAS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MIGRATESTATEFORMAT;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_COLL_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_REPLICA_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOCK_SHARD_TASK;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MOVEREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REBALANCELEADERS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REINDEXCOLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RELOAD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RENAME;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REPLACENODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.RESTORE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.SPLITSHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.UTILIZENODE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.util.Utils.makeMap;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.cloud.LockTree;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerMessageHandler;
import org.apache.solr.cloud.OverseerNodePrioritizer;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerTaskProcessor;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkController.NotInClusterStateException;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related
 * overseer messages.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler, SolrCloseable {

  public static final String NUM_SLICES = "numShards";

  public static final boolean CREATE_NODE_SET_SHUFFLE_DEFAULT = true;
  public static final String CREATE_NODE_SET_SHUFFLE = CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
  public static final String CREATE_NODE_SET_EMPTY = "EMPTY";
  public static final String CREATE_NODE_SET = CollectionAdminParams.CREATE_NODE_SET_PARAM;

  public static final String ROUTER = "router";

  public static final String SHARDS_PROP = "shards";

  public static final String REQUESTID = "requestid";

  public static final String COLL_PROP_PREFIX = "property.";

  public static final String ONLY_IF_DOWN = "onlyIfDown";

  public static final String SHARD_UNIQUE = "shardUnique";

  public static final String ONLY_ACTIVE_NODES = "onlyactivenodes";

  static final String SKIP_CREATE_REPLICA_IN_CLUSTER_STATE = "skipCreateReplicaInClusterState";

  public static final Map<String, Object> COLLECTION_PROPS_AND_DEFAULTS = Collections.unmodifiableMap(makeMap(
      ROUTER, DocRouter.DEFAULT_NAME,
      ZkStateReader.REPLICATION_FACTOR, "1",
      ZkStateReader.NRT_REPLICAS, "1",
      ZkStateReader.TLOG_REPLICAS, "0",
      ZkStateReader.PULL_REPLICAS, "0",
      ZkStateReader.MAX_SHARDS_PER_NODE, "1",
      ZkStateReader.AUTO_ADD_REPLICAS, "false",
      DocCollection.RULE, null,
      POLICY, null,
      SNITCH, null,
      WITH_COLLECTION, null,
      COLOCATED_WITH, null));

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String FAILURE_FIELD = "failure";
  public static final String SUCCESS_FIELD = "success";

  Overseer overseer;
  HttpShardHandlerFactory shardHandlerFactory;
  String adminPath;
  ZkStateReader zkStateReader;
  SolrCloudManager cloudManager;
  String myId;
  Stats stats;
  TimeSource timeSource;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  final private LockTree lockTree = new LockTree();
  ExecutorService tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS,
      new SynchronousQueue<>(),
      new DefaultSolrThreadFactory("OverseerCollectionMessageHandlerThreadFactory"));

  protected static final Random RANDOM;
  static {
    // We try to make things reproducible in the context of our tests by initializing the random instance
    // based on the current seed
    String seed = System.getProperty("tests.seed");
    if (seed == null) {
      RANDOM = new Random();
    } else {
      RANDOM = new Random(seed.hashCode());
    }
  }

  final Map<CollectionAction, Cmd> commandMap;

  private volatile boolean isClosed;

  public OverseerCollectionMessageHandler(ZkStateReader zkStateReader, String myId,
                                        final HttpShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Stats stats,
                                        Overseer overseer,
                                        OverseerNodePrioritizer overseerPrioritizer) {
    this.zkStateReader = zkStateReader;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    this.cloudManager = overseer.getSolrCloudManager();
    this.timeSource = cloudManager.getTimeSource();
    this.isClosed = false;
    commandMap = new ImmutableMap.Builder<CollectionAction, Cmd>()
        .put(REPLACENODE, new ReplaceNodeCmd(this))
        .put(DELETENODE, new DeleteNodeCmd(this))
        .put(BACKUP, new BackupCmd(this))
        .put(RESTORE, new RestoreCmd(this))
        .put(CREATESNAPSHOT, new CreateSnapshotCmd(this))
        .put(DELETESNAPSHOT, new DeleteSnapshotCmd(this))
        .put(SPLITSHARD, new SplitShardCmd(this))
        .put(ADDROLE, new OverseerRoleCmd(this, ADDROLE, overseerPrioritizer))
        .put(REMOVEROLE, new OverseerRoleCmd(this, REMOVEROLE, overseerPrioritizer))
        .put(MOCK_COLL_TASK, this::mockOperation)
        .put(MOCK_SHARD_TASK, this::mockOperation)
        .put(MOCK_REPLICA_TASK, this::mockOperation)
        .put(MIGRATESTATEFORMAT, this::migrateStateFormat)
        .put(CREATESHARD, new CreateShardCmd(this))
        .put(MIGRATE, new MigrateCmd(this))
        .put(CREATE, new CreateCollectionCmd(this))
        .put(MODIFYCOLLECTION, this::modifyCollection)
        .put(ADDREPLICAPROP, this::processReplicaAddPropertyCommand)
        .put(DELETEREPLICAPROP, this::processReplicaDeletePropertyCommand)
        .put(BALANCESHARDUNIQUE, this::balanceProperty)
        .put(REBALANCELEADERS, this::processRebalanceLeaders)
        .put(RELOAD, this::reloadCollection)
        .put(DELETE, new DeleteCollectionCmd(this))
        .put(CREATEALIAS, new CreateAliasCmd(this))
        .put(DELETEALIAS, new DeleteAliasCmd(this))
        .put(ALIASPROP, new SetAliasPropCmd(this))
        .put(MAINTAINROUTEDALIAS, new MaintainRoutedAliasCmd(this))
        .put(OVERSEERSTATUS, new OverseerStatusCmd(this))
        .put(DELETESHARD, new DeleteShardCmd(this))
        .put(DELETEREPLICA, new DeleteReplicaCmd(this))
        .put(ADDREPLICA, new AddReplicaCmd(this))
        .put(MOVEREPLICA, new MoveReplicaCmd(this))
        .put(REINDEXCOLLECTION, new ReindexCollectionCmd(this))
        .put(UTILIZENODE, new UtilizeNodeCmd(this))
        .put(RENAME, new RenameCmd(this))
        .build()
    ;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SolrResponse processMessage(ZkNodeProps message, String operation) {
    if (log.isDebugEnabled()) {
      log.debug("processMessage(ZkNodeProps message={}, String operation={}) - start", message, operation);
    }

    MDCLoggingContext.setCollection(message.getStr(COLLECTION));
    MDCLoggingContext.setShard(message.getStr(SHARD_ID_PROP));
    MDCLoggingContext.setReplica(message.getStr(REPLICA_PROP));
    
    NamedList results = new NamedList();
    try {
      CollectionAction action = getCollectionAction(operation);
      Cmd command = commandMap.get(action);
      if (command != null) {
        command.call(cloudManager.getClusterStateProvider().getClusterState(), message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }
    } catch (Exception e) {
      log.error("processMessage(ZkNodeProps=" + message + ", String=" + operation + ")", e);

      DW.propegateInterrupt(e);
      
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr(NAME);

      if (collName == null) {
        SolrException.log(log, "Operation " + operation + " failed", e);
      } else  {
        SolrException.log(log, "Collection: " + collName + " operation: " + operation
            + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap nl = new SimpleOrderedMap();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    }
    SolrResponse returnSolrResponse = new OverseerSolrResponse(results);
    if (log.isDebugEnabled()) {
      log.debug("processMessage(ZkNodeProps, String) - end");
    }
    return returnSolrResponse;
  }

  @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
  private void mockOperation(ClusterState state, ZkNodeProps message, NamedList results) throws InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("mockOperation(ClusterState state={}, ZkNodeProps message={}, NamedList results={}) - start", state, message, results);
    }

    //only for test purposes
    Thread.sleep(message.getInt("sleep", 1));
    log.info("MOCK_TASK_EXECUTED time {} data {}", System.currentTimeMillis(), Utils.toJSONString(message));
    results.add("MOCK_FINISHED", System.currentTimeMillis());

    if (log.isDebugEnabled()) {
      log.debug("mockOperation(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  private CollectionAction getCollectionAction(String operation) {
    if (log.isDebugEnabled()) {
      log.debug("getCollectionAction(String operation={}) - start", operation);
    }

    CollectionAction action = CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }

    if (log.isDebugEnabled()) {
      log.debug("getCollectionAction(String) - end");
    }
    return action;
  }

  private void reloadCollection(ClusterState clusterState, ZkNodeProps message, NamedList results) {
    if (log.isDebugEnabled()) {
      log.debug("reloadCollection(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.RELOAD.toString());

    String asyncId = message.getStr(ASYNC);
    collectionCmd(message, params, results, Replica.State.ACTIVE, asyncId);

    if (log.isDebugEnabled()) {
      log.debug("reloadCollection(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  @SuppressWarnings("unchecked")
  private void processRebalanceLeaders(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("processRebalanceLeaders(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP,
        CORE_NODE_NAME_PROP, BASE_URL_PROP, REJOIN_AT_HEAD_PROP);

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
    params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
    params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REJOINLEADERELECTION.toString());
    params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
    params.set(CORE_NODE_NAME_PROP, message.getStr(CORE_NODE_NAME_PROP));
    params.set(ELECTION_NODE_PROP, message.getStr(ELECTION_NODE_PROP));
    params.set(BASE_URL_PROP, message.getStr(BASE_URL_PROP));

    String baseUrl = message.getStr(BASE_URL_PROP);
    ShardRequest sreq = new ShardRequest();
    sreq.nodeName = message.getStr(ZkStateReader.CORE_NAME_PROP);
    // yes, they must use same admin handler path everywhere...
    params.set("qt", adminPath);
    sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
    sreq.shards = new String[] {baseUrl};
    sreq.actualShards = sreq.shards;
    sreq.params = params;
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler(overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient());
    shardHandler.submit(sreq, baseUrl, sreq.params);

    if (log.isDebugEnabled()) {
      log.debug("processRebalanceLeaders(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  @SuppressWarnings("unchecked")
  private void processReplicaAddPropertyCommand(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("processReplicaAddPropertyCommand(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    overseer.offerStateUpdate(Utils.toJSON(m));

    if (log.isDebugEnabled()) {
      log.debug("processReplicaAddPropertyCommand(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  private void processReplicaDeletePropertyCommand(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("processReplicaDeletePropertyCommand(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map<String, Object> propMap = new HashMap<>();
    propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
    propMap.putAll(message.getProperties());
    ZkNodeProps m = new ZkNodeProps(propMap);
    overseer.offerStateUpdate(Utils.toJSON(m));

    if (log.isDebugEnabled()) {
      log.debug("processReplicaDeletePropertyCommand(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  private void balanceProperty(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("balanceProperty(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
              "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
    }
    SolrZkClient zkClient = zkStateReader.getZkClient();
    Map<String, Object> m = new HashMap<>();
    m.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
    m.putAll(message.getProperties());
    if (log.isDebugEnabled()) {
      log.debug("send message to state update queue {}", m);
    }
    overseer.offerStateUpdate(Utils.toJSON(m));

    if (log.isDebugEnabled()) {
      log.debug("balanceProperty(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  /**
   * Get collection status from cluster state.
   * Can return collection status by given shard name.
   *
   *
   * @param collection collection map parsed from JSON-serialized {@link ClusterState}
   * @param name  collection name
   * @param requestedShards a set of shards to be returned in the status.
   *                        An empty or null values indicates <b>all</b> shards.
   * @return map of collection properties
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getCollectionStatus(Map<String, Object> collection, String name, Set<String> requestedShards) {
    if (log.isDebugEnabled()) {
      log.debug("getCollectionStatus(Map<String,Object> collection={}, String name={}, Set<String> requestedShards={}) - start", collection, name, requestedShards);
    }

    if (collection == null)  {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
    }
    if (requestedShards == null || requestedShards.isEmpty()) {
      if (log.isDebugEnabled()) {
        log.debug("getCollectionStatus(Map<String,Object>, String, Set<String>) - end");
      }
      return collection;
    } else {
      Map<String, Object> shards = (Map<String, Object>) collection.get("shards");
      Map<String, Object>  selected = new HashMap<>();
      for (String selectedShard : requestedShards) {
        if (!shards.containsKey(selectedShard)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + name + " shard: " + selectedShard + " not found");
        }
        selected.put(selectedShard, shards.get(selectedShard));
        collection.put("shards", selected);
      }

      if (log.isDebugEnabled()) {
        log.debug("getCollectionStatus(Map<String,Object>, String, Set<String>) - end");
      }
      return collection;
    }
  }

  @SuppressWarnings("unchecked")
  void deleteReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("deleteReplica(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}, Runnable onComplete={}) - start", clusterState, message, results, onComplete);
    }

    ((DeleteReplicaCmd) commandMap.get(DELETEREPLICA)).deleteReplica(clusterState, message, results, onComplete);

    if (log.isDebugEnabled()) {
      log.debug("deleteReplica(ClusterState, ZkNodeProps, NamedList, Runnable) - end");
    }
  }

  boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("waitForCoreNodeGone(String collectionName={}, String shard={}, String replicaName={}, int timeoutms={}) - start", collectionName, shard, replicaName, timeoutms);
    }

    try {
      zkStateReader.waitForState(collectionName, timeoutms, TimeUnit.MILLISECONDS, (c) -> {
          if (c == null)
            return true;
          Slice slice = c.getSlice(shard);
          if(slice == null || slice.getReplica(replicaName) == null) {
            return true;
          }
          return false;
        });
    } catch (TimeoutException e) {
      log.error("waitForCoreNodeGone(String=" + collectionName + ", String=" + shard + ", String=" + replicaName + ", int=" + timeoutms + ")", e);

      if (log.isDebugEnabled()) {
        log.debug("waitForCoreNodeGone(String, String, String, int) - end");
      }
      return false;
    }

    if (log.isDebugEnabled()) {
      log.debug("waitForCoreNodeGone(String, String, String, int) - end");
    }
    return true;
  }

  void deleteCoreNode(String collectionName, String replicaName, Replica replica, String core) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("deleteCoreNode(String collectionName={}, String replicaName={}, Replica replica={}, String core={}) - start", collectionName, replicaName, replica, core);
    }

    ZkNodeProps m = new ZkNodeProps(
        Overseer.QUEUE_OPERATION, OverseerAction.DELETECORE.toLower(),
        ZkStateReader.CORE_NAME_PROP, core,
        ZkStateReader.NODE_NAME_PROP, replica.getStr(ZkStateReader.NODE_NAME_PROP),
        ZkStateReader.COLLECTION_PROP, collectionName,
        ZkStateReader.CORE_NODE_NAME_PROP, replicaName,
        ZkStateReader.BASE_URL_PROP, replica.getStr(ZkStateReader.BASE_URL_PROP));
    if (log.isDebugEnabled()) {
      log.debug("send message to state update queue {}", m);
    }
    overseer.offerStateUpdate(Utils.toJSON(m));

    if (log.isDebugEnabled()) {
      log.debug("deleteCoreNode(String, String, Replica, String) - end");
    }
  }

  void checkRequired(ZkNodeProps message, String... props) {
    if (log.isDebugEnabled()) {
      log.debug("checkRequired(ZkNodeProps message={}, String props={}) - start", message, props);
    }

    for (String prop : props) {
      if(message.get(prop) == null){
        throw new SolrException(ErrorCode.BAD_REQUEST, StrUtils.join(Arrays.asList(props),',') +" are required params" );
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("checkRequired(ZkNodeProps, String) - end");
    }
  }

  void checkResults(String label, NamedList<Object> results, boolean failureIsFatal) throws SolrException {
    if (log.isDebugEnabled()) {
      log.debug("checkResults(String label={}, NamedList<Object> results={}, boolean failureIsFatal={}) - start", label, results, failureIsFatal);
    }

    Object failure = results.get("failure");
    if (failure == null) {
      failure = results.get("error");
    }
    if (failure != null) {
      String msg = "Error: " + label + ": " + Utils.toJSONString(results);
      if (failureIsFatal) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
      } else {
        log.error(msg);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("checkResults(String, NamedList<Object>, boolean) - end");
    }
  }


  //TODO should we not remove in the next release ?
  private void migrateStateFormat(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("migrateStateFormat(ClusterState state={}, ZkNodeProps message={}, NamedList results={}) - start", state, message, results);
    }

    final String collectionName = message.getStr(COLLECTION_PROP);

    boolean firstLoop = true;
    // wait for a while until the state format changes
    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, timeSource);
    while (! timeout.hasTimedOut()) {
      DocCollection collection = zkStateReader.getClusterState().getCollection(collectionName);
      if (collection == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection: " + collectionName + " not found");
      }
      if (collection.getStateFormat() == 2) {
        // Done.
        results.add("success", new SimpleOrderedMap<>());

        if (log.isDebugEnabled()) {
          log.debug("migrateStateFormat(ClusterState, ZkNodeProps, NamedList) - end");
        }
        return;
      }

      if (firstLoop) {
        // Actually queue the migration command.
        firstLoop = false;
        ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, MIGRATESTATEFORMAT.toLower(), COLLECTION_PROP, collectionName);
        if (log.isDebugEnabled()) {
          log.debug("send message to state update queue {}", m);
        }
        overseer.offerStateUpdate(Utils.toJSON(m));
      }
      timeout.sleep(100);
    }
    throw new SolrException(ErrorCode.SERVER_ERROR, "Could not migrate state format for collection: " + collectionName);
  }

  void commit(NamedList results, String slice, Replica parentShardLeader) {
    log.debug("Calling soft commit to make sub shard updates visible");
    String coreUrl = new ZkCoreNodeProps(parentShardLeader).getCoreUrl();
    // HttpShardHandler is hard coded to send a QueryRequest hence we go direct
    // and we force open a searcher so that we have documents to show upon switching states
    UpdateResponse updateResponse = null;
    try {
      updateResponse = softCommit(coreUrl);
      processResponse(results, null, coreUrl, updateResponse, slice, Collections.emptySet());
    } catch (Exception e) {
      log.error("commit(NamedList=" + results + ", String=" + slice + ", Replica=" + parentShardLeader + ")", e);
      DW.propegateInterrupt(e);
      processResponse(results, e, coreUrl, updateResponse, slice, Collections.emptySet());
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to call distrib softCommit on: " + coreUrl, e);
    }

    if (log.isDebugEnabled()) {
      log.debug("commit(NamedList, String, Replica) - end");
    }
  }


  static UpdateResponse softCommit(String url) throws SolrServerException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("softCommit(String url={}) - start", url);
    }

    try (HttpSolrClient client = new HttpSolrClient.Builder(url)
        .withConnectionTimeout(30000)
        .withSocketTimeout(120000)
        .build()) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.setParams(new ModifiableSolrParams());
      ureq.setAction(AbstractUpdateRequest.ACTION.COMMIT, false, true, true);
      UpdateResponse returnUpdateResponse = ureq.process(client);
      if (log.isDebugEnabled()) {
        log.debug("softCommit(String) - end");
      }
      return returnUpdateResponse;
    }
  }

  String waitForCoreNodeName(String collectionName, String msgNodeName, String msgCore) {
    AtomicReference<String> errorMessage = new AtomicReference<>();
    AtomicReference<String> coreNodeName = new AtomicReference<>();
    try {
      zkStateReader.waitForState(collectionName, 320, TimeUnit.SECONDS, (n, c) -> {
        if (c == null)
          return false;
        final Map<String,Slice> slicesMap = c.getSlicesMap();
        for (Slice slice : slicesMap.values()) {
          for (Replica replica : slice.getReplicas()) {

            String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
            String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

            if (msgNodeName.equals(nodeName) && core.equals(msgCore)) {
              coreNodeName.set(replica.getName());
              return true;
            }
          }
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      String error = errorMessage.get();
      if (error == null)
        error = "Timeout waiting for collection state.";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }
    
    return coreNodeName.get();
  }

  void waitForNewShard(String collectionName, String sliceName) throws KeeperException, InterruptedException {
    log.debug("Waiting for slice {} of collection {} to be available", sliceName, collectionName);
    try {
      zkStateReader.waitForState(collectionName, 320, TimeUnit.SECONDS, (n, c) -> {
        if (c == null)
          return false;
        Slice slice = c.getSlice(sliceName);
        if (slice != null) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException | InterruptedException e) {
      String error = "Timeout waiting for new shard.";
      throw new NotInClusterStateException(ErrorCode.SERVER_ERROR, error);
    }
  }

  DocRouter.Range intersect(DocRouter.Range a, DocRouter.Range b) {
    if (log.isDebugEnabled()) {
      log.debug("intersect(DocRouter.Range a={}, DocRouter.Range b={}) - start", a, b);
    }

    if (a == null || b == null || !a.overlaps(b)) {
      if (log.isDebugEnabled()) {
        log.debug("intersect(DocRouter.Range, DocRouter.Range) - end");
      }
      return null;
    } else if (a.isSubsetOf(b)) {
      if (log.isDebugEnabled()) {
        log.debug("intersect(DocRouter.Range, DocRouter.Range) - end");
      }
      return a;
    } else if (b.isSubsetOf(a)) {
      if (log.isDebugEnabled()) {
        log.debug("intersect(DocRouter.Range, DocRouter.Range) - end");
      }
      return b;
    } else if (b.includes(a.max)) {
      DocRouter.Range returnRange = new DocRouter.Range(b.min, a.max);
      if (log.isDebugEnabled()) {
        log.debug("intersect(DocRouter.Range, DocRouter.Range) - end");
      }
      return returnRange;
    } else  {
      DocRouter.Range returnRange = new DocRouter.Range(a.min, b.max);
      if (log.isDebugEnabled()) {
        log.debug("intersect(DocRouter.Range, DocRouter.Range) - end");
      }
      return returnRange;
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


  private void modifyCollection(ClusterState clusterState, ZkNodeProps message, NamedList results)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("modifyCollection(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}) - start", clusterState, message, results);
    }

    final String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
    //the rest of the processing is based on writing cluster state properties
    //remove the property here to avoid any errors down the pipeline due to this property appearing
    String configName = (String) message.getProperties().remove(CollectionAdminParams.COLL_CONF);

    if(configName != null) {
      validateConfigOrThrowSolrException(configName);

      boolean isLegacyCloud =  Overseer.isLegacy(zkStateReader);
      createConfNode(cloudManager.getDistribStateManager(), configName, collectionName, isLegacyCloud);
      reloadCollection(null, new ZkNodeProps(NAME, collectionName), results);
    }

    if (log.isDebugEnabled()) {
      log.debug("send message to state update queue {}", message);
    }
    overseer.offerStateUpdate(Utils.toJSON(message));
    
    try {
      zkStateReader.waitForState(collectionName, 30, TimeUnit.SECONDS, (n, c) -> {
        if (c == null) return false;

        for (Map.Entry<String,Object> updateEntry : message.getProperties().entrySet()) {
          String updateKey = updateEntry.getKey();

          if (!updateKey.equals(ZkStateReader.COLLECTION_PROP)
              && !updateKey.equals(Overseer.QUEUE_OPERATION)
              && updateEntry.getValue() != null // handled below in a separate conditional
              && !updateEntry.getValue().equals(c.get(updateKey))) {
            return false;
          }

          if (updateEntry.getValue() == null && c.containsKey(updateKey)) {
            return false;
          }
        }
        return true;
      });
    } catch (TimeoutException | InterruptedException e) {
      log.error("modifyCollection(ClusterState=" + clusterState + ", ZkNodeProps=" + message + ", NamedList=" + results + ")", e);
      throw new DW.Exp("Could not modify collection " + message, e);
    }

    // if switching to/from read-only mode reload the collection
    if (message.keySet().contains(ZkStateReader.READ_ONLY)) {
      reloadCollection(null, new ZkNodeProps(NAME, collectionName), results);
    }

    if (log.isDebugEnabled()) {
      log.debug("modifyCollection(ClusterState, ZkNodeProps, NamedList) - end");
    }
  }

  void cleanupCollection(String collectionName, NamedList results) throws Exception {
    log.error("Cleaning up collection [" + collectionName + "]." );
    Map<String, Object> props = makeMap(
        Overseer.QUEUE_OPERATION, DELETE.toLower(),
        NAME, collectionName);
    commandMap.get(DELETE).call(zkStateReader.getClusterState(), new ZkNodeProps(props), results);

    if (log.isDebugEnabled()) {
      log.debug("cleanupCollection(String, NamedList) - end");
    }
  }

  Map<String, Replica> waitToSeeReplicasInState(String collectionName, Collection<String> coreNames) throws InterruptedException {
    AtomicReference<Map<String, Replica>> result = new AtomicReference<>();
    AtomicReference<String> errorMessage = new AtomicReference<>();
    try {
      zkStateReader.waitForState(collectionName, 30, TimeUnit.SECONDS, (n, c) -> {
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


  List<ZkNodeProps> addReplica(ClusterState clusterState, ZkNodeProps message, NamedList results, Runnable onComplete)
      throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("addReplica(ClusterState clusterState={}, ZkNodeProps message={}, NamedList results={}, Runnable onComplete={}) - start", clusterState, message, results, onComplete);
    }

    List<ZkNodeProps> returnList = ((AddReplicaCmd) commandMap.get(ADDREPLICA)).addReplica(clusterState, message, results, onComplete);
    if (log.isDebugEnabled()) {
      log.debug("addReplica(ClusterState, ZkNodeProps, NamedList, Runnable) - end");
    }
    return returnList;
  }

  void validateConfigOrThrowSolrException(String configName) throws IOException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("validateConfigOrThrowSolrException(String configName={}) - start", configName);
    }

    boolean isValid = cloudManager.getDistribStateManager().hasData(ZkConfigManager.CONFIGS_ZKNODE + "/" + configName);
    if(!isValid) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not find the specified config set: " + configName);
    }

    if (log.isDebugEnabled()) {
      log.debug("validateConfigOrThrowSolrException(String) - end");
    }
  }

  /**
   * This doesn't validate the config (path) itself and is just responsible for creating the confNode.
   * That check should be done before the config node is created.
   */
  public static void createConfNode(DistribStateManager stateManager, String configName, String coll, boolean isLegacyCloud) throws IOException, AlreadyExistsException, BadVersionException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("createConfNode(DistribStateManager stateManager={}, String configName={}, String coll={}, boolean isLegacyCloud={}) - start", stateManager, configName, coll, isLegacyCloud);
    }

    if (configName != null) {
      String collDir = ZkStateReader.COLLECTIONS_ZKNODE + "/" + coll;
      log.debug("creating collections conf node {} ", collDir);
      byte[] data = Utils.toJSON(makeMap(ZkController.CONFIGNAME_PROP, configName));
      if (stateManager.hasData(collDir)) {
        stateManager.setData(collDir, data, -1);
      } else {
        stateManager.makePath(collDir, data, CreateMode.PERSISTENT, false);
      }
    } else {
      if(isLegacyCloud){
        log.warn("Could not obtain config name={}", configName);
      } else {
        log.error("Unable to get config name");
        throw new SolrException(ErrorCode.BAD_REQUEST,"Unable to get config name=" + configName);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("createConfNode(DistribStateManager, String, String, boolean) - end");
    }
  }

  private List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                             NamedList<Object> results, Replica.State stateMatcher, String asyncId) {
    if (log.isDebugEnabled()) {
      log.debug("collectionCmd(ZkNodeProps message={}, ModifiableSolrParams params={}, NamedList<Object> results={}, Replica.State stateMatcher={}, String asyncId={}) - start", message, params, results, stateMatcher, asyncId);
    }

    List<Replica> returnList = collectionCmd(message, params, results, stateMatcher, asyncId, Collections.emptySet());
    if (log.isDebugEnabled()) {
      log.debug("collectionCmd(ZkNodeProps, ModifiableSolrParams, NamedList<Object>, Replica.State, String) - end");
    }
    return returnList;
  }

  /**
   * Send request to all replicas of a collection
   * @return List of replicas which is not live for receiving the request
   */
  List<Replica> collectionCmd(ZkNodeProps message, ModifiableSolrParams params,
                     NamedList<Object> results, Replica.State stateMatcher, String asyncId, Set<String> okayExceptions) {
    log.info("Executing Collection Cmd={}, asyncId={}", params, asyncId);
    String collectionName = message.getStr(NAME);
    @SuppressWarnings("deprecation")
    ShardHandler shardHandler = shardHandlerFactory.getShardHandler(overseer.getCoreContainer().getUpdateShardHandler().getDefaultHttpClient());

    ClusterState clusterState = zkStateReader.getClusterState();
    DocCollection coll = clusterState.getCollection(collectionName);
    List<Replica> notLivesReplicas = new ArrayList<>();
    final ShardRequestTracker shardRequestTracker = new ShardRequestTracker(asyncId);
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(shardRequestTracker.sliceCmd(clusterState, params, stateMatcher, slice, shardHandler));
    }

    shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);

    if (log.isDebugEnabled()) {
      log.debug("collectionCmd(ZkNodeProps, ModifiableSolrParams, NamedList<Object>, Replica.State, String, Set<String>) - end");
    }
    return notLivesReplicas;
  }

  private void processResponse(NamedList<Object> results, ShardResponse srsp, Set<String> okayExceptions) {
    if (log.isDebugEnabled()) {
      log.debug("processResponse(NamedList<Object> results={}, ShardResponse srsp={}, Set<String> okayExceptions={}) - start", results, srsp, okayExceptions);
    }

    Throwable e = srsp.getException();
    String nodeName = srsp.getNodeName();
    SolrResponse solrResponse = srsp.getSolrResponse();
    String shard = srsp.getShard();

    processResponse(results, e, nodeName, solrResponse, shard, okayExceptions);

    if (log.isDebugEnabled()) {
      log.debug("processResponse(NamedList<Object>, ShardResponse, Set<String>) - end");
    }
  }

  @SuppressWarnings("deprecation")
  private void processResponse(NamedList<Object> results, Throwable e, String nodeName, SolrResponse solrResponse, String shard, Set<String> okayExceptions) {
    if (log.isDebugEnabled()) {
      log.debug("processResponse(NamedList<Object> results={}, Throwable e={}, String nodeName={}, SolrResponse solrResponse={}, String shard={}, Set<String> okayExceptions={}) - start", results, e, nodeName, solrResponse, shard, okayExceptions);
    }

    String rootThrowable = null;
    if (e instanceof RemoteSolrException) {
      rootThrowable = ((RemoteSolrException) e).getRootThrowable();
    }

    if (e != null && (rootThrowable == null || !okayExceptions.contains(rootThrowable))) {
      log.error("Error from shard: " + shard, e);
      addFailure(results, nodeName, e.getClass().getName() + ":" + e.getMessage());
    } else {
      addSuccess(results, nodeName, solrResponse.getResponse());
    }

    if (log.isDebugEnabled()) {
      log.debug("processResponse(NamedList<Object>, Throwable, String, SolrResponse, String, Set<String>) - end");
    }
  }

  @SuppressWarnings("unchecked")
  private static void addFailure(NamedList<Object> results, String key, Object value) {
    if (log.isDebugEnabled()) {
      log.debug("addFailure(NamedList<Object> results={}, String key={}, Object value={}) - start", results, key, value);
    }

    SimpleOrderedMap<Object> failure = (SimpleOrderedMap<Object>) results.get("failure");
    if (failure == null) {
      failure = new SimpleOrderedMap<>();
      results.add("failure", failure);
    }
    failure.add(key, value);

    if (log.isDebugEnabled()) {
      log.debug("addFailure(NamedList<Object>, String, Object) - end");
    }
  }

  @SuppressWarnings("unchecked")
  private static void addSuccess(NamedList<Object> results, String key, Object value) {
    if (log.isDebugEnabled()) {
      log.debug("addSuccess(NamedList<Object> results={}, String key={}, Object value={}) - start", results, key, value);
    }

    SimpleOrderedMap<Object> success = (SimpleOrderedMap<Object>) results.get("success");
    if (success == null) {
      success = new SimpleOrderedMap<>();
      results.add("success", success);
    }
    success.add(key, value);

    if (log.isDebugEnabled()) {
      log.debug("addSuccess(NamedList<Object>, String, Object) - end");
    }
  }

  private NamedList<Object> waitForCoreAdminAsyncCallToComplete(String nodeName, String requestId) {
    if (log.isDebugEnabled()) {
      log.debug("waitForCoreAdminAsyncCallToComplete(String nodeName={}, String requestId={}) - start", nodeName, requestId);
    }

    ShardHandler shardHandler = shardHandlerFactory.getShardHandler();
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminAction.REQUESTSTATUS.toString());
    params.set(CoreAdminParams.REQUESTID, requestId);
    int counter = 0;
    ShardRequest sreq;
    do {
      sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          NamedList<Object> results = new NamedList<>();
          processResponse(results, srsp, Collections.emptySet());
          if (srsp.getSolrResponse().getResponse() == null) {
            NamedList<Object> response = new NamedList<>();
            response.add("STATUS", "failed");

            if (log.isDebugEnabled()) {
              log.debug("waitForCoreAdminAsyncCallToComplete(String, String) - end");
            }
            return response;
          }

          String r = (String) srsp.getSolrResponse().getResponse().get("STATUS");
          if (r.equals("running")) {
            log.debug("The task is still RUNNING, continuing to wait.");
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              log.error("waitForCoreAdminAsyncCallToComplete(String=" + nodeName + ", String=" + requestId + ")", e);

              Thread.currentThread().interrupt();
            }
            continue;

          } else if (r.equals("completed")) {
            log.debug("The task is COMPLETED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("failed")) {
            // TODO: Improve this. Get more information.
            log.debug("The task is FAILED, returning");
            return srsp.getSolrResponse().getResponse();
          } else if (r.equals("notfound")) {
            log.debug("The task is notfound, retry");
            if (counter++ < 5) {
              try {
                Thread.sleep(1000);
              } catch (InterruptedException e) {
                log.warn("waitForCoreAdminAsyncCallToComplete(String=" + nodeName + ", String=" + requestId + ") - exception ignored", e);
              }
              break;
            }
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request for requestId: " + requestId + "" + srsp.getSolrResponse().getResponse().get("STATUS") +
                "retried " + counter + "times");
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid status request " + srsp.getSolrResponse().getResponse().get("STATUS"));
          }
        }
      } while (srsp != null);
    } while(true);
  }

  @Override
  public String getName() {
    return "Overseer Collection Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    if (log.isDebugEnabled()) {
      log.debug("getTimerName(String operation={}) - start", operation);
    }

    String returnString = "collection_" + operation;
    if (log.isDebugEnabled()) {
      log.debug("getTimerName(String) - end");
    }
    return returnString;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    if (log.isDebugEnabled()) {
      log.debug("getTaskKey(ZkNodeProps message={}) - start", message);
    }

    String returnString = message.containsKey(COLLECTION_PROP) ? message.getStr(COLLECTION_PROP) : message.getStr(NAME);
    if (log.isDebugEnabled()) {
      log.debug("getTaskKey(ZkNodeProps) - end");
    }
    return returnString;
  }


  private long sessionId = -1;
  private LockTree.Session lockSession;

  @Override
  public Lock lockTask(ZkNodeProps message, OverseerTaskProcessor.TaskBatch taskBatch) {
    if (log.isDebugEnabled()) {
      log.debug("lockTask(ZkNodeProps message={}, OverseerTaskProcessor.TaskBatch taskBatch={}) - start", message, taskBatch);
    }

    if (lockSession == null || sessionId != taskBatch.getId()) {
      //this is always called in the same thread.
      //Each batch is supposed to have a new taskBatch
      //So if taskBatch changes we must create a new Session
      // also check if the running tasks are empty. If yes, clear lockTree
      // this will ensure that locks are not 'leaked'
      if(taskBatch.getRunningTasks() == 0) lockTree.clear();
      lockSession = lockTree.getSession();
    }
    Lock returnLock = lockSession.lock(getCollectionAction(message.getStr(Overseer.QUEUE_OPERATION)),
        Arrays.asList(
            getTaskKey(message),
            message.getStr(ZkStateReader.SHARD_ID_PROP),
            message.getStr(ZkStateReader.REPLICA_PROP))

    );
    if (log.isDebugEnabled()) {
      log.debug("lockTask(ZkNodeProps, OverseerTaskProcessor.TaskBatch) - end");
    }
    return returnLock;
  }


  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    this.isClosed = true;
    if (tpe != null) {
      if (!tpe.isShutdown()) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  protected interface Cmd {
    void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception;
  }

  /*
   * backward compatibility reasons, add the response with the async ID as top level.
   * This can be removed in Solr 9
   */
  @Deprecated
  static boolean INCLUDE_TOP_LEVEL_RESPONSE = true;

  public ShardRequestTracker syncRequestTracker() {
    if (log.isDebugEnabled()) {
      log.debug("syncRequestTracker() - start");
    }

    ShardRequestTracker returnShardRequestTracker = new ShardRequestTracker(null);
    if (log.isDebugEnabled()) {
      log.debug("syncRequestTracker() - end");
    }
    return returnShardRequestTracker;
  }

  public ShardRequestTracker asyncRequestTracker(String asyncId) {
    if (log.isDebugEnabled()) {
      log.debug("asyncRequestTracker(String asyncId={}) - start", asyncId);
    }

    ShardRequestTracker returnShardRequestTracker = new ShardRequestTracker(asyncId);
    if (log.isDebugEnabled()) {
      log.debug("asyncRequestTracker(String) - end");
    }
    return returnShardRequestTracker;
  }

  public class ShardRequestTracker{
    private final String asyncId;
    private final NamedList<String> shardAsyncIdByNode = new NamedList<String>();

    private ShardRequestTracker(String asyncId) {
      this.asyncId = asyncId;
    }

    /**
     * Send request to all replicas of a slice
     * @return List of replicas which is not live for receiving the request
     */
    public List<Replica> sliceCmd(ClusterState clusterState, ModifiableSolrParams params, Replica.State stateMatcher,
                  Slice slice, ShardHandler shardHandler) {
      if (log.isDebugEnabled()) {
        log.debug("sliceCmd(ClusterState clusterState={}, ModifiableSolrParams params={}, Replica.State stateMatcher={}, Slice slice={}, ShardHandler shardHandler={}) - start", clusterState, params, stateMatcher, slice, shardHandler);
      }

      List<Replica> notLiveReplicas = new ArrayList<>();
      for (Replica replica : slice.getReplicas()) {
        if ((stateMatcher == null || Replica.State.getState(replica.getStr(ZkStateReader.STATE_PROP)) == stateMatcher)) {
          if (clusterState.liveNodesContain(replica.getStr(ZkStateReader.NODE_NAME_PROP))) {
            // For thread safety, only simple clone the ModifiableSolrParams
            ModifiableSolrParams cloneParams = new ModifiableSolrParams();
            cloneParams.add(params);
            cloneParams.set(CoreAdminParams.CORE, replica.getStr(ZkStateReader.CORE_NAME_PROP));

            sendShardRequest(replica.getStr(ZkStateReader.NODE_NAME_PROP), cloneParams, shardHandler);
          } else {
            notLiveReplicas.add(replica);
          }
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("sliceCmd(ClusterState, ModifiableSolrParams, Replica.State, Slice, ShardHandler) - end");
      }
      return notLiveReplicas;
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params,
        ShardHandler shardHandler) {
      if (log.isDebugEnabled()) {
        log.debug("sendShardRequest(String nodeName={}, ModifiableSolrParams params={}, ShardHandler shardHandler={}) - start", nodeName, params, shardHandler);
      }

      sendShardRequest(nodeName, params, shardHandler, adminPath, zkStateReader);

      if (log.isDebugEnabled()) {
        log.debug("sendShardRequest(String, ModifiableSolrParams, ShardHandler) - end");
      }
    }

    public void sendShardRequest(String nodeName, ModifiableSolrParams params, ShardHandler shardHandler,
        String adminPath, ZkStateReader zkStateReader) {
      if (log.isDebugEnabled()) {
        log.debug("sendShardRequest(String nodeName={}, ModifiableSolrParams params={}, ShardHandler shardHandler={}, String adminPath={}, ZkStateReader zkStateReader={}) - start", nodeName, params, shardHandler, adminPath, zkStateReader);
      }

      if (asyncId != null) {
        String coreAdminAsyncId = asyncId + Math.abs(System.nanoTime());
        params.set(ASYNC, coreAdminAsyncId);
        track(nodeName, coreAdminAsyncId);
      }

      ShardRequest sreq = new ShardRequest();
      params.set("qt", adminPath);
      sreq.purpose = 1;
      String replica = zkStateReader.getBaseUrlForNodeName(nodeName);
      sreq.shards = new String[] {replica};
      sreq.actualShards = sreq.shards;
      sreq.nodeName = nodeName;
      sreq.params = params;

      shardHandler.submit(sreq, replica, sreq.params);

      if (log.isDebugEnabled()) {
        log.debug("sendShardRequest(String, ModifiableSolrParams, ShardHandler, String, ZkStateReader) - end");
      }
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError) {
      if (log.isDebugEnabled()) {
        log.debug("processResponses(NamedList<Object> results={}, ShardHandler shardHandler={}, boolean abortOnError={}, String msgOnError={}) - start", results, shardHandler, abortOnError, msgOnError);
      }

      processResponses(results, shardHandler, abortOnError, msgOnError, Collections.emptySet());

      if (log.isDebugEnabled()) {
        log.debug("processResponses(NamedList<Object>, ShardHandler, boolean, String) - end");
      }
    }

    void processResponses(NamedList<Object> results, ShardHandler shardHandler, boolean abortOnError, String msgOnError,
        Set<String> okayExceptions) {
      if (log.isDebugEnabled()) {
        log.debug("processResponses(NamedList<Object> results={}, ShardHandler shardHandler={}, boolean abortOnError={}, String msgOnError={}, Set<String> okayExceptions={}) - start", results, shardHandler, abortOnError, msgOnError, okayExceptions);
      }

      // Processes all shard responses
      ShardResponse srsp;
      do {
        srsp = shardHandler.takeCompletedOrError();
        if (srsp != null) {
          processResponse(results, srsp, okayExceptions);
          Throwable exception = srsp.getException();
          if (abortOnError && exception != null) {
            // drain pending requests
            while (srsp != null) {
              srsp = shardHandler.takeCompletedOrError();
            }
            throw new SolrException(ErrorCode.SERVER_ERROR, msgOnError, exception);
          }
        }
      } while (srsp != null);

      // If request is async wait for the core admin to complete before returning
      if (asyncId != null) {
        waitForAsyncCallsToComplete(results); // TODO: Shouldn't we abort with msgOnError exception when failure?
        shardAsyncIdByNode.clear();
      }

      if (log.isDebugEnabled()) {
        log.debug("processResponses(NamedList<Object>, ShardHandler, boolean, String, Set<String>) - end");
      }
    }

    private void waitForAsyncCallsToComplete(NamedList<Object> results) {
      if (log.isDebugEnabled()) {
        log.debug("waitForAsyncCallsToComplete(NamedList<Object> results={}) - start", results);
      }

      for (Map.Entry<String,String> nodeToAsync:shardAsyncIdByNode) {
        final String node = nodeToAsync.getKey();
        final String shardAsyncId = nodeToAsync.getValue();
        log.debug("I am Waiting for :{}/{}", node, shardAsyncId);
        NamedList<Object> reqResult = waitForCoreAdminAsyncCallToComplete(node, shardAsyncId);
        if (INCLUDE_TOP_LEVEL_RESPONSE) {
          results.add(shardAsyncId, reqResult);
        }
        if ("failed".equalsIgnoreCase(((String)reqResult.get("STATUS")))) {
          log.error("Error from shard {}: {}", node,  reqResult);
          addFailure(results, node, reqResult);
        } else {
          addSuccess(results, node, reqResult);
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("waitForAsyncCallsToComplete(NamedList<Object>) - end");
      }
    }

    /** @deprecated consider to make it private after {@link CreateCollectionCmd} refactoring*/
    @Deprecated void track(String nodeName, String coreAdminAsyncId) {
      if (log.isDebugEnabled()) {
        log.debug("track(String nodeName={}, String coreAdminAsyncId={}) - start", nodeName, coreAdminAsyncId);
      }

      shardAsyncIdByNode.add(nodeName, coreAdminAsyncId);

      if (log.isDebugEnabled()) {
        log.debug("track(String, String) - end");
      }
    }
  }
}
