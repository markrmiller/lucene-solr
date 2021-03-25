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

package org.apache.solr.handler.admin;

import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler.CallInfo;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;


public class PrepRecoveryOp implements CoreAdminHandler.CoreAdminOp {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void execute(CallInfo it) throws Exception {

    final SolrParams params = it.req.getParams();

    String cname = params.required().get(CoreAdminParams.CORE);

    String leaderName = params.required().get("leaderName");

    String collection = params.get("collection");

    String shard = params.get(ZkStateReader.SHARD_ID_PROP);

    String state = params.get(ZkStateReader.STATE_PROP);

    Replica.State waitForState = null;
    if (state != null) {
      waitForState = Replica.State.getState(state);
    }

    log.debug(
        "Going to wait for core: {}, state: {}: params={}",
        cname, waitForState, params);

    LeaderElector leaderElector = it.handler.coreContainer.getZkController().getLeaderElector(leaderName);
    if (leaderElector == null || !leaderElector.isLeader()) {
      throw new NotValidLeader("Not the valid leader (replica=" + leaderName + ")" + (leaderElector == null ? "No leader elector" : "Elector state=" + leaderElector.getState()) +
          " coll=" + collection);
    }

    if (waitForState == null) {
      log.debug("Done checking leader:", leaderName);
      return;
    }

    assert TestInjection.injectPrepRecoveryOpPauseForever();

    CoreContainer coreContainer = it.handler.coreContainer;

    AtomicReference<String> errorMessage = new AtomicReference<>();

    try {
      Replica.State finalWaitForState = waitForState;
      coreContainer.getZkController().getZkStateReader().waitForState(collection, 5, TimeUnit.SECONDS, (n, c) -> {
        if (c == null) {
          errorMessage.set(
              "Timeout waiting to see " + finalWaitForState + " state replica=" + cname
                  + " waitForState=" + finalWaitForState);
          return false;
        }

        // wait until we are sure the recovering node is ready
        // to accept updates
        final Replica replica = c.getReplica(cname);
        boolean isLive = false;
        if (replica == null) {
          errorMessage.set(
              "Timeout waiting to see " + finalWaitForState + " state replica=" + cname + " state=" + (replica == null ? "(null replica)" : replica.getState())
                  + " waitForState=" + finalWaitForState + " isLive=" + isLive);
          return false;
        }

        isLive = coreContainer.getZkController().getZkStateReader().isNodeLive(replica.getNodeName());
        if (isLive) {
          if (replica.getState() == finalWaitForState) {
            if (log.isDebugEnabled()) {
              log.debug("replica={} state={} waitForState={} isLive={}", replica, replica.getState(), finalWaitForState,
                  coreContainer.getZkController().getZkStateReader().isNodeLive(replica.getNodeName()));
            }
            return true;
          } else if (replica.getState() == Replica.State.ACTIVE) {
            return true;
          }
        }

        errorMessage.set(
            "Timeout waiting to see " + finalWaitForState + " state replica=" + cname + " state=" + (replica == null ? "(null replica)" : replica.getState())
                + " waitForState=" + finalWaitForState + " isLive=" + isLive);
        return false;
      });

    } catch (TimeoutException | InterruptedException e) {
      ParWork.propagateInterrupt(e);
      String error = errorMessage.get();
      log.error("", error);
    }
  }

  public static class NotValidLeader extends SolrException {

    public NotValidLeader(String s) {
      super(ErrorCode.BAD_REQUEST, s);
    }
  }
}
