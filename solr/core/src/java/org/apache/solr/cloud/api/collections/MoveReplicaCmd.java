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

import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.update.UpdateLog;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.SKIP_CREATE_REPLICA_IN_CLUSTER_STATE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.IN_PLACE_MOVE;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class MoveReplicaCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;

  public MoveReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
  }

  @Override
  public CollectionCmdResponse.Response call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
    return moveReplica(ocmh.zkStateReader.getClusterState(), message, results);
  }

  private CollectionCmdResponse.Response moveReplica(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("moveReplica() : {}", Utils.toJSONString(message));
    }
    ocmh.checkRequired(message, COLLECTION_PROP, CollectionParams.TARGET_NODE);
    String extCollection = message.getStr(COLLECTION_PROP);
    String targetNode = message.getStr(CollectionParams.TARGET_NODE);
    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    boolean inPlaceMove = message.getBool(IN_PLACE_MOVE, true);
    int timeout = message.getInt(TIMEOUT, 10 * 60); // 10 minutes

    String async = message.getStr(ASYNC);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collection;
    if (followAliases) {
      collection = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollection);
    } else {
      collection = extCollection;
    }

    DocCollection coll = clusterState.getCollection(collection);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " does not exist");
    }
    if (!ocmh.zkStateReader.getLiveNodes().contains(targetNode)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Target node: " + targetNode + " not in live nodes: " + ocmh.zkStateReader.getLiveNodes());
    }
    Replica replica = null;
    if (message.containsKey(REPLICA_PROP)) {
      String replicaName = message.getStr(REPLICA_PROP);
      replica = coll.getReplica(replicaName);
      if (replica == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " replica: " + replicaName + " does not exist");
      }
    } else {
      String sourceNode = message.getStr(CollectionParams.SOURCE_NODE, message.getStr(CollectionParams.FROM_NODE));
      if (sourceNode == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'" + CollectionParams.SOURCE_NODE + " or '" + CollectionParams.FROM_NODE + "' is a required param");
      }
      String shardId = message.getStr(SHARD_ID_PROP);
      if (shardId == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'" + SHARD_ID_PROP + "' is a required param");
      }
      Slice slice = coll.getSlice(shardId);
      List<Replica> sliceReplicas = new ArrayList<>(slice.getReplicas(r -> sourceNode.equals(r.getNodeName())));
      if (sliceReplicas.isEmpty()) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection +
            " node: " + sourceNode + " does not have any replica belonging to shard: " + shardId + " collection=" + coll);
      }
      Collections.shuffle(sliceReplicas, OverseerCollectionMessageHandler.RANDOM);
      replica = sliceReplicas.iterator().next();
    }

    if (coll.getStr(CollectionAdminParams.COLOCATED_WITH) != null) {
      // we must ensure that moving this replica does not cause the co-location to break
      String sourceNode = replica.getNodeName();
      String colocatedCollectionName = coll.getStr(CollectionAdminParams.COLOCATED_WITH);
      DocCollection colocatedCollection = clusterState.getCollectionOrNull(colocatedCollectionName);
      if (colocatedCollection != null) {
        if (colocatedCollection.getReplica((s, r) -> sourceNode.equals(r.getNodeName())) != null) {
          // check if we have at least two replicas of the collection on the source node
          // only then it is okay to move one out to another node
          List<Replica> replicasOnSourceNode = coll.getReplicas(replica.getNodeName());
          if (replicasOnSourceNode == null || replicasOnSourceNode.size() < 2) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Collection: " + collection + " is co-located with collection: " + colocatedCollectionName + " and has a single replica: " + replica.getName() + " on node: " + replica.getNodeName()
                    + " so it is not possible to move it to another node");
          }
        }
      }
    }

    log.info("Replica will be moved to node {}: {}", targetNode, replica);
    Slice slice = null;
    for (Slice s : coll.getSlices()) {
      if (s.getReplicas().contains(replica)) {
        slice = s;
      }
    }
    assert slice != null;
    Object dataDir = replica.get("dataDir");
    boolean isSharedFS = replica.getBool(ZkStateReader.SHARED_STORAGE_PROP, false) && dataDir != null;

    CollectionCmdResponse.Response resp = null;
    if (isSharedFS && inPlaceMove) {
      log.debug("-- moveHdfsReplica");
      // MRM TODO: TODO
      moveHdfsReplica(clusterState, results, dataDir.toString(), targetNode, async, coll, replica, slice, timeout, waitForFinalState);
    } else {
      log.debug("-- moveNormalReplica (inPlaceMove={}, isSharedFS={}", inPlaceMove, isSharedFS);
      resp = moveNormalReplica(clusterState, results, targetNode, async, coll, replica, slice, timeout, waitForFinalState);
    }

    CollectionCmdResponse.Response response = new CollectionCmdResponse.Response();

    OverseerCollectionMessageHandler.Finalize finalizer = resp.asyncFinalRunner;
    response.asyncFinalRunner = new MyFinalize(finalizer);

    response.clusterState = resp.clusterState;

    return response;
  }

  @SuppressWarnings({"unchecked"})
  private void moveHdfsReplica(ClusterState clusterState, @SuppressWarnings({"rawtypes"}) NamedList results, String dataDir, String targetNode, String async, DocCollection coll, Replica replica,
      Slice slice, int timeout, boolean waitForFinalState) throws Exception {
    String skipCreateReplicaInClusterState = "true";
    if (ocmh.zkStateReader.getLiveNodes().contains(replica.getNodeName())) {
      skipCreateReplicaInClusterState = "false";
      ZkNodeProps removeReplicasProps = new ZkNodeProps(COLLECTION_PROP, coll.getName(), SHARD_ID_PROP, slice.getName(), REPLICA_PROP, replica.getName());
      removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_DATA_DIR, false);
      removeReplicasProps.getProperties().put(CoreAdminParams.DELETE_INDEX, false);
      if (async != null) removeReplicasProps.getProperties().put(ASYNC, async);
      @SuppressWarnings({"rawtypes"}) NamedList deleteResult = new NamedList();
      try {
        ocmh.deleteReplica(clusterState, removeReplicasProps, deleteResult);
      } catch (SolrException e) {
        // assume this failed completely so there's nothing to roll back
        deleteResult.add("failure", e.toString());
      }
      if (deleteResult.get("failure") != null) {
        String errorString = String
            .format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s, failure=%s", coll.getName(), slice.getName(), replica.getName(), deleteResult.get("failure"));
        log.warn(errorString);
        results.add("failure", errorString);
        return;
      }

      TimeOut timeOut = new TimeOut(20L, TimeUnit.SECONDS, timeSource);
      while (!timeOut.hasTimedOut()) {
        coll = ocmh.zkStateReader.getClusterState().getCollection(coll.getName());
        if (coll.getReplica(replica.getName()) != null) {
          timeOut.sleep(100);
        } else {
          break;
        }
      }
      if (timeOut.hasTimedOut()) {
        results.add("failure", "Still see deleted replica in clusterstate!");
        return;
      }

    }

    String ulogDir = replica.getStr(CoreAdminParams.ULOG_DIR);
    ZkNodeProps addReplicasProps = new ZkNodeProps(COLLECTION_PROP, coll.getName(), SHARD_ID_PROP, slice.getName(), CoreAdminParams.NODE, targetNode, CoreAdminParams.NAME, replica.getName(),
        WAIT_FOR_FINAL_STATE, String.valueOf(waitForFinalState), SKIP_CREATE_REPLICA_IN_CLUSTER_STATE, skipCreateReplicaInClusterState, CoreAdminParams.ULOG_DIR,
        ulogDir.substring(0, ulogDir.lastIndexOf(UpdateLog.TLOG_NAME)), CoreAdminParams.DATA_DIR, dataDir, ZkStateReader.REPLICA_TYPE, replica.getType().name());

    if (async != null) addReplicasProps.getProperties().put(ASYNC, async);
    @SuppressWarnings({"rawtypes"}) NamedList addResult = new NamedList();
    try {
      ocmh.addReplica(ocmh.zkStateReader.getClusterState(), addReplicasProps, addResult);
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      // fatal error - try rolling back
      String errorString = String
          .format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" + " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
      results.add("failure", errorString);
      log.warn("Error adding replica {} - trying to roll back...", addReplicasProps, e);
      addReplicasProps = addReplicasProps.plus(CoreAdminParams.NODE, replica.getNodeName());
      @SuppressWarnings({"rawtypes"}) NamedList rollback = new NamedList();
      clusterState = ocmh.addReplica(clusterState, addReplicasProps, rollback).clusterState;
      if (rollback.get("failure") != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica + ", collection may be inconsistent: " + rollback.get("failure"));
      }
      return;
    }
    if (addResult.get("failure") != null) {
      String errorString = String
          .format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" + " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
      log.warn(errorString);
      results.add("failure", errorString);
      log.debug("--- trying to roll back...");
      // try to roll back
      addReplicasProps = addReplicasProps.plus(CoreAdminParams.NODE, replica.getNodeName());
      addReplicasProps = addReplicasProps.plus(WAIT_FOR_FINAL_STATE, true);
      @SuppressWarnings({"rawtypes"}) NamedList rollback = new NamedList();
      try {
        ocmh.addReplica(ocmh.zkStateReader.getClusterState(), addReplicasProps, rollback);
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica + ", collection may be inconsistent!", e);
      }
      if (rollback.get("failure") != null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Fatal error during MOVEREPLICA of " + replica + ", collection may be inconsistent! Failure: " + rollback.get("failure"));
      }
      return;
    } else {
      String successString = String
          .format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " + "to replica=%s at node=%s", replica.getName(), replica.getNodeName(), replica.getName(),
              targetNode);
      results.add("success", successString);
    }
  }

  @SuppressWarnings({"unchecked"})
  private CollectionCmdResponse.Response moveNormalReplica(ClusterState clusterState, @SuppressWarnings({"rawtypes"}) NamedList results, String targetNode, String async, DocCollection coll,
      Replica replica, Slice slice, int timeout, boolean waitForFinalState) throws Exception {
    String newCoreName = Assign.buildSolrCoreName(coll, slice.getName(), replica.getType(), ocmh.overseer).coreName;

    ZkNodeProps addReplicasProps = new ZkNodeProps(COLLECTION_PROP, coll.getName(), SHARD_ID_PROP, slice.getName(), CoreAdminParams.NODE, targetNode, CoreAdminParams.NAME, newCoreName,
        ZkStateReader.REPLICA_TYPE, replica.getType().name(), WAIT_FOR_FINAL_STATE, "true");

    if (async != null) addReplicasProps.getProperties().put(ASYNC, async + "-AddReplica-" + Math.abs(System.nanoTime()));
    @SuppressWarnings({"rawtypes"}) NamedList addResult = new NamedList();
    SolrCloseableLatch countDownLatch = new SolrCloseableLatch(1, ocmh);

    CollectionCmdResponse.Response response = ocmh.addReplicaWithResp(clusterState, addReplicasProps, addResult);

    // wait for the other replica to be active if the source replica was a leader

    CollectionCmdResponse.Response finalResponse = new CollectionCmdResponse.Response();
    
   // finalResponse.clusterState = response.clusterState;

    finalResponse.asyncFinalRunner = () -> {
      log.debug("Waiting for leader's replica to recover.");

      response.asyncFinalRunner.call();

      if (addResult.get("failure") != null) {
        String errorString = String
            .format(Locale.ROOT, "Failed to create replica for collection=%s shard=%s" + " on node=%s, failure=%s", coll.getName(), slice.getName(), targetNode, addResult.get("failure"));
        log.warn(errorString);
        results.add("failure", errorString);

        CollectionCmdResponse.Response response1 = new CollectionCmdResponse.Response();
        return response1;
      } else {

        ocmh.zkStateReader.waitForState(coll.getName(), 1, TimeUnit.HOURS, (liveNodes, collectionState) -> {
          if (collectionState == null) {
            return false;
          }
          Replica rep = collectionState.getReplica(newCoreName);
          if (rep == null) {
            return false;
          }

          if (rep.getState() != Replica.State.ACTIVE) {
            return false;
          }
          return true;
        });

        CollectionCmdResponse.Response response1 = new CollectionCmdResponse.Response();
        CollectionCmdResponse.Response asyncResp = new CollectionCmdResponse.Response();
        ZkNodeProps removeReplicasProps = new ZkNodeProps(COLLECTION_PROP, coll.getName(), SHARD_ID_PROP, slice.getName(), REPLICA_PROP, replica.getName(), WAIT_FOR_FINAL_STATE, "true");
        if (async != null) removeReplicasProps.getProperties().put(ASYNC, async);
        @SuppressWarnings({"rawtypes"}) NamedList deleteResult = new NamedList();
        try {
          response1.clusterState = ocmh.deleteReplica(clusterState, removeReplicasProps, deleteResult).clusterState;
          String collection = response1.clusterState.getCollectionsMap().keySet().iterator().next();
          ocmh.overseer.getZkStateWriter().enqueueStructureChange(response1.clusterState.getCollection(collection));
          asyncResp.writeFuture = ocmh.overseer.writePendingUpdates(collection);
        } catch (SolrException e) {
          deleteResult.add("failure", e.toString());
        }
        if (deleteResult.get("failure") != null) {
          String errorString = String.format(Locale.ROOT, "Failed to cleanup replica collection=%s shard=%s name=%s, failure=%s", coll.getName(), slice.getName(), replica.getName(), deleteResult.get("failure"));
          log.warn(errorString);
          response1.asyncFinalRunner.call();
          results.add("failure", errorString);
        } else {
          String successString = String
              .format(Locale.ROOT, "MOVEREPLICA action completed successfully, moved replica=%s at node=%s " + "to replica=%s at node=%s", replica.getName(), replica.getNodeName(), newCoreName,
                  targetNode);
          results.add("success", successString);
        }

        return asyncResp;
      }
    };
    
    return finalResponse;
  }

  private static class MyFinalize implements OverseerCollectionMessageHandler.Finalize {
    private final OverseerCollectionMessageHandler.Finalize finalizer;

    public MyFinalize(OverseerCollectionMessageHandler.Finalize finalizer) {
      this.finalizer = finalizer;
    }

    @Override
    public CollectionCmdResponse.Response call() {
      if (finalizer != null) {
        try {
          finalizer.call();
        } catch (Exception e) {
          log.error("Exception during MoveReplica", e);
        }
      }
      CollectionCmdResponse.Response response = new CollectionCmdResponse.Response();
      return response;
    }
  }
}
