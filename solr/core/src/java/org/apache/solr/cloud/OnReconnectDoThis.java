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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.SolrClientCloudManager;
import org.apache.solr.client.solrj.impl.ZkClientClusterStateProvider;
import org.apache.solr.cloud.ZkController.OnReconnectNotifyAsync;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.patterns.SW;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.KeeperException.SessionExpiredException;

final class OnReconnectDoThis implements OnReconnect {

  /**
   * 
   */
  private final ZkController zkController;

  OnReconnectDoThis(ZkController zkController) {
    this.zkController = zkController;

  }

  @Override
  public void command() throws SessionExpiredException {
    ZkController.log.info("ZooKeeper session re-connected ... refreshing core states after session expiration.");
    zkController.clearZkCollectionTerms();

    // recreate our watchers first so that they exist even on any problems below
    zkController.getZkStateReader().createClusterStateWatchersAndUpdate();

    // this is troublesome - we dont want to kill anything the old
    // leader accepted
    // though I guess sync will likely get those updates back? But
    // only if
    // he is involved in the sync, and he certainly may not be
    // ExecutorUtil.shutdownAndAwaitTermination(cc.getCmdDistribExecutor());
    // we need to create all of our lost watches

    // seems we dont need to do this again...
    // Overseer.createClientNodes(zkClient, getNodeName());

    // start the overseer first as following code may need it's processing

    // nocommit
//    ElectionContext context = new OverseerElectionContext(zkController.getNodeName(), zkController.zkClient, zkController.overseer);
//    try {
//      ElectionContext prevContext = zkController.overseerElector.getContext();
//      if (prevContext != null) {
//
//        prevContext.cancelElection();
//
//        prevContext.close();
//
//      }
//      zkController.overseerElector.setup(context);
//      zkController.overseerElector.joinElection(context, true);
//
//    } catch (Exception e) {
//      throw new DW.Exp(e);
//    }

    // we have to register as live first to pick up docs in the buffer
    zkController.createEphemeralLiveNode();

    List<CoreDescriptor> descriptors = zkController.registerOnReconnect.getCurrentDescriptors();
    // re register all descriptors

    if (descriptors != null) {
      try (SW worker = new SW(this)) {

        for (CoreDescriptor descriptor : descriptors) {
          // TODO: we need to think carefully about what happens when it
          // was
          // a leader that was expired - as well as what to do about
          // leaders/overseers
          // with connection loss

          worker.collect(() -> {
            // unload solrcores that have been 'failed over'
            // nocommt - we want to do this without corecontainer access ...
//            try {
//              zkController.throwErrorIfReplicaReplaced(descriptor);
//            } catch (SolrException e) {
//              ZkController.log.info(e.getMessage());
//            }

            try {
              zkController.register(descriptor.getName(), descriptor, true, true, false);
            } catch (Exception e) {
              SW.propegateInterrupt("Solr encountered an unexpected exception!", e);
            }
          });

        }
        worker.addCollect("Reregister cores");
      }
    }

//    zkController.cloudSolrClient = new CloudSolrClient.Builder(new ZkClientClusterStateProvider(zkController.zkStateReader))
//        .withSocketTimeout(30000).withConnectionTimeout(15000)
//        .withHttpClient(zkController.cc.getUpdateShardHandler().getDefaultHttpClient())
//        .withConnectionTimeout(15000).withSocketTimeout(30000).build();
//    zkController.cloudManager = new SolrClientCloudManager(new ZkDistributedQueueFactory(zkController.zkClient), zkController.cloudSolrClient);
//    zkController.cloudManager.getClusterStateProvider().connect();

    // nocommit - use DW
    // notify any other objects that need to know when the session was re-connected

    try (SW worker = new SW(this)) {

      // the OnReconnect operation can be expensive per listener, so do that async in the background
      zkController.reconnectListeners.forEach((it) -> {
        worker.collect(new ZkController.OnReconnectNotifyAsync(it));

        worker.addCollect("ReconnectListeners");
      });

    }

  }
}