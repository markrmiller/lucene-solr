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
package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;

import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.patterns.DW;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkCmdExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private long retryDelay = 1500L; // 1 second would match timeout, so 500 ms over for padding
  private int retryCount;
  private double timeouts;
  private IsClosed isClosed;
  
  public ZkCmdExecutor(int timeoutms) {
    this(timeoutms, null);
  }
  
  /**
   * TODO: At this point, this should probably take a SolrZkClient in
   * its constructor.
   * 
   * @param timeoutms
   *          the client timeout for the ZooKeeper clients that will be used
   *          with this class.
   */
  public ZkCmdExecutor(int timeoutms, IsClosed isClosed) {
    timeouts = timeoutms / 1000.0;
    this.retryCount = Math.round(0.5f * ((float)Math.sqrt(8.0f * timeouts + 1.0f) - 1.0f)) + 1;
    this.isClosed = isClosed;
  }
  
  private boolean isClosed() {
    return isClosed != null && isClosed.isClosed();
  }

  public void ensureExists(String path, final SolrZkClient zkClient) {
    ensureExists(path, null, CreateMode.PERSISTENT, zkClient, 0);
  }
  
  
  public void ensureExists(String path, final byte[] data, final SolrZkClient zkClient)  {
    ensureExists(path, data, CreateMode.PERSISTENT, zkClient, 0);
  }
  
  public void ensureExists(String path, final byte[] data, CreateMode createMode, final SolrZkClient zkClient) {
    ensureExists(path, data, createMode, zkClient, 0);
  }

  public void ensureExists(final String path, final byte[] data,
      CreateMode createMode, final SolrZkClient zkClient, int skipPathParts) {

    try {
      if (zkClient.exists(path, true)) {
        return;
      }

      try {
        zkClient.makePath(path, data, createMode, null, true, true, skipPathParts);
      } catch (NodeExistsException e) {
        // it's okay if another beats us creating the node
      }
    } catch (Exception e1) {
      throw new DW.Exp(e1);
    }
  }
  
  /**
   * Performs a retry delay if this is not the first attempt
   * 
   * @param attemptCount
   *          the number of the attempts performed so far
   */
  protected void retryDelay(int attemptCount) throws InterruptedException {
    Thread.sleep((attemptCount + 1) * retryDelay);
  }

}
