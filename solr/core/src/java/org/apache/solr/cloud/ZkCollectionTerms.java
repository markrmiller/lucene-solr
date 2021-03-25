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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.zookeeper.KeeperException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Used to manage all ZkShardTerms of a collection
 */
class ZkCollectionTerms implements AutoCloseable {
  private final String collection;
  private final Map<String,ZkShardTerms> terms;
  private final SolrZkClient zkClient;

  private ReentrantLock shardTermsLock = new ReentrantLock();

  private volatile boolean closed;

  ZkCollectionTerms(String collection, SolrZkClient client) {
    this.collection = collection;
    this.terms = new ConcurrentHashMap<>();
    this.zkClient = client;
    assert ObjectReleaseTracker.track(this);
  }

  ZkShardTerms getShard(String shardId) throws Exception {

    ZkShardTerms zkterms = terms.get(shardId);

    return zkterms;
  }

  public ZkShardTerms getShardOrNull(String shardId) {
    if (!terms.containsKey(shardId)) return null;
    return terms.get(shardId);
  }

  public void register(String shardId, String name) throws Exception {

    ZkShardTerms zkterms = terms.get(shardId);

    if (zkterms == null) {
      shardTermsLock.lock();
      try {
        zkterms = terms.get(shardId);
        if (zkterms == null) {
          zkterms = new ZkShardTerms(collection, shardId, zkClient);
          terms.put(shardId, zkterms);
        }

      } finally {
        shardTermsLock.unlock();
      }
    }
    zkterms.registerTerm(name);
  }

  public void remove(String shardId, String name) throws KeeperException, InterruptedException {
    ZkShardTerms zterms = getShardOrNull(shardId);
    if (zterms != null) {
      if (zterms.removeTermFor(name)) {
        IOUtils.closeQuietly(terms.remove(shardId));
      }
    }
  }

  public void close() {
    closed = true;
    terms.values().forEach(ZkShardTerms::close);
    terms.clear();
    assert ObjectReleaseTracker.release(this);
  }

  public void disableRemoveWatches() {
    terms.values().forEach(ZkShardTerms::disableRemoveWatches);
  }

  public boolean cleanUp() {
    for (ZkShardTerms zkShardTerms : terms.values()) {
      if (zkShardTerms.getTerms().size() > 0) {
        return false;
      }
    }
    return true;
  }
}
