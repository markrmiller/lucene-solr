
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.admin.MetricsHistoryHandler;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

public class DeleteCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final CoreContainer coreContainer;
  private String adminPath;

  public DeleteCollectionCmd(CoreContainer coreContainer, String adminPath) {
    this.coreContainer = coreContainer;
    this.adminPath = adminPath;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    Object o = message.get(MaintainRoutedAliasCmd.INVOKED_BY_ROUTED_ALIAS);
    if (o != null) {
      ((Runnable)o).run(); // this will ensure the collection is removed from the alias before it disappears.
    }
    final String extCollection = message.getStr(NAME);
    ZkStateReader zkStateReader = coreContainer.getZkController().getZkStateReader();

    if (zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      zkStateReader.aliasesManager.update(); // aliases may have been stale; get latest from ZK
    }

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    List<String> aliasReferences = checkAliasReference(zkStateReader, extCollection, followAliases);

    Aliases aliases = zkStateReader.getAliases();

    String collection;
    if (followAliases) {
      collection = aliases.resolveSimpleAlias(extCollection);
    } else {
      collection = extCollection;
    }

    checkNotColocatedWith(zkStateReader, collection);

    final boolean deleteHistory = message.getBool(CoreAdminParams.DELETE_METRICS_HISTORY, true);

    boolean removeCounterNode = true;
    try {
      // Remove the snapshots meta-data for this collection in ZK. Deleting actual index files
      // should be taken care of as part of collection delete operation.
      SolrZkClient zkClient = zkStateReader.getZkClient();
      SolrSnapshotManager.cleanupCollectionLevelSnapshots(zkClient, collection);

      if (zkStateReader.getClusterState().getCollectionOrNull(collection) == null) {
        if (zkStateReader.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection, true)) {
          // if the collection is not in the clusterstate, but is listed in zk, do nothing, it will just
          // be removed in the finally - we cannot continue, because the below code will error if the collection
          // is not in the clusterstate
          return;
        }
      }
      // remove collection-level metrics history
      if (deleteHistory) {
        MetricsHistoryHandler historyHandler = coreContainer.getMetricsHistoryHandler();
        if (historyHandler != null) {
          String registry = SolrMetricManager.getRegistryName(SolrInfoBean.Group.collection, collection);
          historyHandler.removeHistory(registry);
        }
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);
      params.set(CoreAdminParams.DELETE_METRICS_HISTORY, deleteHistory);

      String asyncId = message.getStr(ASYNC);

      Set<String> okayExceptions = new HashSet<>(1);
      okayExceptions.add(NonExistentCoreException.class.getName());
      ZkNodeProps internalMsg = message.plus(NAME, collection);

      List<Replica> failedReplicas = collectionCmd(internalMsg, params, results, null, asyncId, okayExceptions);
      for (Replica failedReplica : failedReplicas) {
        boolean isSharedFS = failedReplica.getBool(ZkStateReader.SHARED_STORAGE_PROP, false) && failedReplica.get("dataDir") != null;
        if (isSharedFS) {
          // if the replica use a shared FS and it did not receive the unload message, then counter node should not be removed
          // because when a new collection with same name is created, new replicas may reuse the old dataDir
          removeCounterNode = false;
          break;
        }
      }

      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETE.toLower(), NAME, collection);
      coreContainer.getZkController().sendStateUpdate(m);

      // wait for a while until we don't see the collection
      zkStateReader.waitForState(collection, 60, TimeUnit.SECONDS, (collectionState) -> collectionState == null);

      // we can delete any remaining unique aliases
      if (!aliasReferences.isEmpty()) {
        coreContainer.getZkController().getZkStateReader().aliasesManager.applyModificationAndExportToZk(a -> {
          for (String alias : aliasReferences) {
            a = a.cloneWithCollectionAlias(alias, null);
          }
          return a;
        });
      }

//      TimeOut timeout = new TimeOut(60, TimeUnit.SECONDS, timeSource);
//      boolean removed = false;
//      while (! timeout.hasTimedOut()) {
//        timeout.sleep(100);
//        removed = !zkStateReader.getClusterState().hasCollection(collection);
//        if (removed) {
//          timeout.sleep(500); // just a bit of time so it's more likely other
//          // readers see on return
//          break;
//        }
//      }
//      if (!removed) {
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
//            "Could not fully remove collection: " + collection);
//      }
    } finally {

      try {
        String collectionPath =  ZkStateReader.getCollectionPathRoot(collection);
        if (zkStateReader.getZkClient().exists(collectionPath, true)) {
          if (removeCounterNode) {
            zkStateReader.getZkClient().clean(collectionPath);
          } else {
            final String counterNodePath = Assign.getCounterNodePath(collection);
            zkStateReader.getZkClient().clean(collectionPath, s -> !s.equals(counterNodePath));
          }
        }
      } catch (InterruptedException e) {
        SolrException.log(log, "Cleaning up collection in zk was interrupted:"
            + collection, e);
        Thread.currentThread().interrupt();
      } catch (KeeperException e) {
        SolrException.log(log, "Problem cleaning up collection in zk:"
            + collection, e);
      }
    }
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
    ShardHandler shardHandler = ((HttpShardHandlerFactory) coreContainer.getShardHandlerFactory()).getShardHandler(coreContainer.getUpdateShardHandler().getDefaultHttpClient());

    ClusterState clusterState = coreContainer.getZkController().getZkStateReader().getClusterState();
    DocCollection coll = clusterState.getCollection(collectionName);
    List<Replica> notLivesReplicas = new ArrayList<>();
    final ShardRequestTracker shardRequestTracker = new ShardRequestTracker(adminPath, coreContainer,
        coreContainer.getZkController().getZkStateReader(), asyncId);
    for (Slice slice : coll.getSlices()) {
      notLivesReplicas.addAll(shardRequestTracker.sliceCmd(clusterState, params, stateMatcher, slice, shardHandler));
    }

    shardRequestTracker.processResponses(results, shardHandler, false, null, okayExceptions);

    if (log.isDebugEnabled()) {
      log.debug("collectionCmd(ZkNodeProps, ModifiableSolrParams, NamedList<Object>, Replica.State, String, Set<String>) - end");
    }
    return notLivesReplicas;
  }

  // This method returns the single collection aliases to delete, if present, or null
  private List<String> checkAliasReference(ZkStateReader zkStateReader, String extCollection, boolean followAliases) throws Exception {
    Aliases aliases = zkStateReader.getAliases();
    List<String> aliasesRefs = referencedByAlias(extCollection, aliases, followAliases);
    List<String> aliasesToDelete = new ArrayList<>();
    if (aliasesRefs.size() > 0) {
      zkStateReader.aliasesManager.update(); // aliases may have been stale; get latest from ZK
      aliases = zkStateReader.getAliases();
      aliasesRefs = referencedByAlias(extCollection, aliases, followAliases);
      String collection = followAliases ? aliases.resolveSimpleAlias(extCollection) : extCollection;
      if (aliasesRefs.size() > 0) {
        for (String alias : aliasesRefs) {
          // for back-compat in 8.x we don't automatically remove other
          // aliases that point only to this collection
          if (!extCollection.equals(alias)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Collection : " + collection + " is part of aliases: " + aliasesRefs + ", remove or modify the aliases before removing this collection.");
          } else {
            aliasesToDelete.add(alias);
          }
        }
      }
    }
    return aliasesToDelete;
  }

  public static List<String> referencedByAlias(String extCollection, Aliases aliases, boolean followAliases) throws IllegalArgumentException {
    Objects.requireNonNull(aliases);
    // this quickly produces error if the name is a complex alias
    String collection = followAliases ? aliases.resolveSimpleAlias(extCollection) : extCollection;
    return aliases.getCollectionAliasListMap().entrySet().stream()
        .filter(e -> !e.getKey().equals(collection))
        .filter(e -> e.getValue().contains(collection) || e.getValue().contains(extCollection))
        .map(Map.Entry::getKey) // alias name
        .collect(Collectors.toList());
  }

  private void checkNotColocatedWith(ZkStateReader zkStateReader, String collection) throws Exception {
    DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (docCollection != null)  {
      String colocatedWith = docCollection.getStr(COLOCATED_WITH);
      if (colocatedWith != null) {
        DocCollection colocatedCollection = zkStateReader.getClusterState().getCollectionOrNull(colocatedWith);
        if (colocatedCollection != null && collection.equals(colocatedCollection.getStr(WITH_COLLECTION))) {
          // todo how do we clean up if reverse-link is not present?
          // can't delete this collection because it is still co-located with another collection
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Collection: " + collection + " is co-located with collection: " + colocatedWith
                  + " remove the link using modify collection API or delete the co-located collection: " + colocatedWith);
        }
      }
    }
  }
}
