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

import static org.junit.Assert.assertNotNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ConnectionManager.IsClosed;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrjNamedThreadFactory;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * All Solr ZooKeeper interactions should go through this class rather than
 * ZooKeeper. This class handles synchronous connects and reconnections.
 *
 */
public class SolrZkClient implements Closeable {

  private static final int MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW = 750;

  static final String NEWL = System.getProperty("line.separator");

  static final int DEFAULT_CLIENT_CONNECT_TIMEOUT = 30000;
  
  static final TransformerFactory transformerFactory = TransformerFactory.newInstance();
  static final Transformer transformer;
  static {
    try {
      transformer = transformerFactory.newTransformer();
    } catch (TransformerConfigurationException e) {
      throw new DW.Exp(e);
    }
    
    transformerFactory.setAttribute("indent-number", 2);

    transformer.setOutputProperty(OutputKeys.INDENT, "yes");

  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private  ConnectionManager connManager;

  private final ZooKeeper keeper;

  private final ZkCmdExecutor zkCmdExecutor;

  private final ExecutorService zkCallbackExecutor = new MDCAwareThreadPoolExecutor(1, Integer.MAX_VALUE,
      15L, TimeUnit.SECONDS,
      new SynchronousQueue<>(true),
      new SolrjNamedThreadFactory("zkCallback"));

  private final ExecutorService zkConnManagerCallbackExecutor =
	      ExecutorUtil.newMDCAwareSingleThreadExecutor(new SolrjNamedThreadFactory("zkConnectionManagerCallback"));

  private volatile boolean isClosed = false;
  private final ZkClientConnectionStrategy zkClientConnectionStrategy;
  private final int zkClientTimeout;
  private  ZkACLProvider zkACLProvider;
  private final String zkServerAddress;

  private final IsClosed higherLevelIsClosed;

  private final CuratorFramework curator;

  private final AsyncCuratorFramework asyncCurator;

  public int getZkClientTimeout() {
    return zkClientTimeout;
  }

  // expert: for tests nocommit - remove
  public SolrZkClient() {
    this.zkServerAddress = null;
    connManager = null;
    zkCmdExecutor = null;
    zkClientTimeout = 0;
    this.curator = null;
    this.asyncCurator = null;
    zkClientConnectionStrategy = null;
    zkACLProvider = null;
    higherLevelIsClosed = null;
    this.keeper = null;
  }
  
  public SolrZkClient(CuratorFramework curator) {
    zkClientConnectionStrategy = null;
    this.zkServerAddress = "";
    log.debug("SolrZkClient(CuratorFramework curator={}) - start", curator);
    this.zkACLProvider = createZkACLProvider();
    higherLevelIsClosed =  null;

    this.curator = curator;
    this.asyncCurator = AsyncCuratorFramework.wrap(curator);
    try {
      this.keeper = this.curator.getZookeeperClient().getZooKeeper();
    } catch (Exception e) {
      log.error("SolrZkClient(CuratorFramework=" + curator + ")", e);

      throw new DW.Exp(e);
    }
    
    this.zkClientTimeout = keeper.getSessionTimeout();
    connManager = null;
    zkCmdExecutor = new ZkCmdExecutor(zkClientTimeout, new IsClosed() {

      @Override
      public boolean isClosed() {
        if (log.isDebugEnabled()) {
          log.debug("$IsClosed.isClosed()");
        }

        boolean returnboolean = SolrZkClient.this.isClosed();

        if (log.isDebugEnabled()) {
          log.debug("$IsClosed.isClosed() - end");
        }
        return returnboolean;
      }
    });
    
    if (log.isDebugEnabled()) {
      log.debug("SolrZkClient(CuratorFramework) - end");
    }
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout) {
    this(zkServerAddress, zkClientTimeout, new DefaultConnectionStrategy(), null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, new DefaultConnectionStrategy(), null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout, OnReconnect onReonnect) {
    this(zkServerAddress, zkClientTimeout, zkClientConnectTimeout, new DefaultConnectionStrategy(), onReonnect);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, DEFAULT_CLIENT_CONNECT_TIMEOUT, strat, onReconnect);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, strat, onReconnect, null, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, BeforeReconnect beforeReconnect) {
    this(zkServerAddress, zkClientTimeout, clientConnectTimeout, strat, onReconnect, beforeReconnect, null, null);
  }

  public SolrZkClient(String zkServerAddress, int zkClientTimeout, int clientConnectTimeout,
      ZkClientConnectionStrategy strat, final OnReconnect onReconnect, BeforeReconnect beforeReconnect, ZkACLProvider zkACLProvider, IsClosed higherLevelIsClosed) {
    this.zkServerAddress = zkServerAddress;
    this.higherLevelIsClosed = higherLevelIsClosed;
    if (strat == null) {
      strat = new DefaultConnectionStrategy();
    }
    this.curator = null;
    this.asyncCurator = null;
    this.zkClientConnectionStrategy = strat;
    this.keeper = null;

    if (!strat.hasZkCredentialsToAddAutomatically()) {
      ZkCredentialsProvider zkCredentialsToAddAutomatically = createZkCredentialsToAddAutomatically();
      strat.setZkCredentialsToAddAutomatically(zkCredentialsToAddAutomatically);
    }

    this.zkClientTimeout = zkClientTimeout;
    // we must retry at least as long as the session timeout
    zkCmdExecutor = new ZkCmdExecutor(zkClientTimeout, new IsClosed() {

      @Override
      public boolean isClosed() {
        if (log.isDebugEnabled()) {
          log.debug("$IsClosed.isClosed()");
        }

        boolean returnboolean = SolrZkClient.this.isClosed();
        return returnboolean;
      }
    });
//    connManager = new ConnectionManager("ZooKeeperConnection Watcher:"
//        + zkServerAddress, this, zkServerAddress, strat, onReconnect, beforeReconnect, new IsClosed() {
//
//          @Override
//          public boolean isClosed() {
//            if (log.isDebugEnabled()) {
//              log.debug("$IsClosed.isClosed() - start");
//            }
//
//            boolean returnboolean = SolrZkClient.this.isClosed();
//            if (log.isDebugEnabled()) {
//              log.debug("$IsClosed.isClosed() - end");
//            }
//            return returnboolean;
//          }
//        });
//
//    try {
//      strat.connect(zkServerAddress, zkClientTimeout, wrapWatcher(connManager),
//          zooKeeper -> {
//            ZooKeeper oldKeeper = keeper;
//            keeper = zooKeeper;
//            try {
//              closeKeeper(oldKeeper);
//            } finally {
//              if (isClosed) {
//                // we may have been closed
//                closeKeeper(SolrZkClient.this.keeper);
//              }
//            }
//          });
//    } catch (Exception e) {
//      log.error("SolrZkClient(String=" + zkServerAddress + ", int=" + zkClientTimeout + ", int=" + clientConnectTimeout + ", ZkClientConnectionStrategy=" + strat + ", OnReconnect=" + onReconnect + ", BeforeReconnect=" + beforeReconnect + ", ZkACLProvider=" + zkACLProvider + ", IsClosed=" + higherLevelIsClosed + ")", e);
//
//      if (e instanceof InterruptedException) {
//        Thread.currentThread().interrupt();
//      }
//      connManager.close();
//      if (keeper != null) {
//        try {
//          keeper.close();
//        } catch (InterruptedException e1) {
//          log.error("SolrZkClient(String=" + zkServerAddress + ", int=" + zkClientTimeout + ", int=" + clientConnectTimeout + ", ZkClientConnectionStrategy=" + strat + ", OnReconnect=" + onReconnect + ", BeforeReconnect=" + beforeReconnect + ", ZkACLProvider=" + zkACLProvider + ", IsClosed=" + higherLevelIsClosed + ")", e1);
//
//          Thread.currentThread().interrupt();
//        }
//      }
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }
//
//    try {
//      connManager.waitForConnected(clientConnectTimeout);
//    } catch (Exception e) {
//      log.error("SolrZkClient(String=" + zkServerAddress + ", int=" + zkClientTimeout + ", int=" + clientConnectTimeout + ", ZkClientConnectionStrategy=" + strat + ", OnReconnect=" + onReconnect + ", BeforeReconnect=" + beforeReconnect + ", ZkACLProvider=" + zkACLProvider + ", IsClosed=" + higherLevelIsClosed + ")", e);
//
//      if (e instanceof InterruptedException) {
//        Thread.currentThread().interrupt();
//      }
//      
//      connManager.close();
//      try {
//        keeper.close();
//      } catch (InterruptedException e1) {
//        log.error("SolrZkClient(String=" + zkServerAddress + ", int=" + zkClientTimeout + ", int=" + clientConnectTimeout + ", ZkClientConnectionStrategy=" + strat + ", OnReconnect=" + onReconnect + ", BeforeReconnect=" + beforeReconnect + ", ZkACLProvider=" + zkACLProvider + ", IsClosed=" + higherLevelIsClosed + ")", e1);
//
//        Thread.currentThread().interrupt();
//      }
//      zkConnManagerCallbackExecutor.shutdownNow();
//      ExecutorUtil.shutdownAndAwaitTermination(zkConnManagerCallbackExecutor);
//      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
//    }
//    assert ObjectReleaseTracker.track(this);
//    if (zkACLProvider == null) {
//      this.zkACLProvider = createZkACLProvider();
//    } else {
//      this.zkACLProvider = zkACLProvider;
//    }
  }

  public ConnectionManager getConnectionManager() {
    return connManager;
  }

  public ZkClientConnectionStrategy getZkClientConnectionStrategy() {
    return zkClientConnectionStrategy;
  }

  public static final String ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkCredentialsProvider";
  protected ZkCredentialsProvider createZkCredentialsToAddAutomatically() {
    if (log.isDebugEnabled()) {
      log.debug("createZkCredentialsToAddAutomatically() - start");
    }

    String zkCredentialsProviderClassName = System.getProperty(ZK_CRED_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkCredentialsProviderClassName)) {
      try {
        //log.info("Using ZkCredentialsProvider: " + zkCredentialsProviderClassName);
        ZkCredentialsProvider returnZkCredentialsProvider = (ZkCredentialsProvider) Class.forName(zkCredentialsProviderClassName).getConstructor().newInstance();
        if (log.isDebugEnabled()) {
          log.debug("createZkCredentialsToAddAutomatically() - end");
        }
        return returnZkCredentialsProvider;
      } catch (Throwable t) {
        log.error("createZkCredentialsToAddAutomatically()", t);

        throw new DW.Exp(t);
      }
    }
    //log.debug("Using default ZkCredentialsProvider");
    ZkCredentialsProvider returnZkCredentialsProvider = new DefaultZkCredentialsProvider();
    if (log.isDebugEnabled()) {
      log.debug("createZkCredentialsToAddAutomatically() - end");
    }
    return returnZkCredentialsProvider;
  }

  public static final String ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME = "zkACLProvider";
  protected ZkACLProvider createZkACLProvider() {
    if (log.isDebugEnabled()) {
      log.debug("createZkACLProvider() - start");
    }

    String zkACLProviderClassName = System.getProperty(ZK_ACL_PROVIDER_CLASS_NAME_VM_PARAM_NAME);
    if (!StringUtils.isEmpty(zkACLProviderClassName)) {
      try {
       // log.info("Using ZkACLProvider: " + zkACLProviderClassName);
        ZkACLProvider returnZkACLProvider = (ZkACLProvider) Class.forName(zkACLProviderClassName).getConstructor().newInstance();
        if (log.isDebugEnabled()) {
          log.debug("createZkACLProvider() - end");
        }
        return returnZkACLProvider;
      } catch (Throwable t) {
        log.error("createZkACLProvider()", t);

        throw new DW.Exp("VM param zkACLProvider does not point to a class implementing ZkACLProvider and with a non-arg constructor", t);
      }
    }
    //log.debug("Using default ZkACLProvider");
    ZkACLProvider returnZkACLProvider = new DefaultZkACLProvider();
    if (log.isDebugEnabled()) {
      log.debug("createZkACLProvider() - end");
    }
    return returnZkACLProvider;
  }

  /**
   * Returns true if client is connected
   */
  public boolean isConnected() {
    if (log.isDebugEnabled()) {
      log.debug("isConnected() - start");
    }

    boolean returnboolean = keeper != null && keeper.getState() == ZooKeeper.States.CONNECTED;
    if (log.isDebugEnabled()) {
      log.debug("isConnected() - end");
    }
    return returnboolean;
  }

  public void delete(final String path, final int version, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) {
      log.debug("delete(String path={}, int version={}, boolean retryOnConnLoss={}) - start", path, version, retryOnConnLoss);
    }

    keeper.delete(path, version);
    
    if (log.isDebugEnabled()) {
      log.debug("delete(String, int, boolean) - end");
    }
  }

  /**
   * Wraps the watcher so that it doesn't fire off ZK's event queue. In order to guarantee that a watch object will
   * only be triggered once for a given notification, users need to wrap their watcher using this method before
   * calling {@link #exists(String, org.apache.zookeeper.Watcher, boolean)} or
   * {@link #getData(String, org.apache.zookeeper.Watcher, org.apache.zookeeper.data.Stat, boolean)}.
   */
  public Watcher wrapWatcher(final Watcher watcher) {
//    if (log.isDebugEnabled()) {
//      log.debug("wrapWatcher(Watcher watcher={}) - start", watcher);
//    }
//
//    if (watcher == null || watcher instanceof ProcessWatchWithExecutor) {
//      if (log.isDebugEnabled()) {
//        log.debug("return (Watcher watcher={}) - start", watcher);
//      }
//      return watcher;
//    }
//    Watcher returnWatcher = new ProcessWatchWithExecutor(watcher);
//    if (log.isDebugEnabled()) {
//      log.debug("wrapWatcher(Watcher) - end");
//    }
    // nocommit
    return watcher;
  }

  /**
   * Return the stat of the node of the given path. Return null if no such a
   * node exists.
   * <p>
   * If the watch is non-null and the call is successful (no exception is thrown),
   * a watch will be left on the node with the given path. The watch will be
   * triggered by a successful operation that creates/delete the node or sets
   * the data on the node.
   *
   * @param path the node path
   * @param watcher explicit watcher
   * @return the stat of the node of the given path; return null if no such a
   *         node exists.
   * @throws KeeperException If the server signals an error
   * @throws InterruptedException If the server transaction is interrupted.
   * @throws IllegalArgumentException if an invalid path is specified
   */
  public Stat exists(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("exists(String path={}, Watcher watcher={}, boolean retryOnConnLoss={}) - start", path, watcher,
          retryOnConnLoss);
    }

    Stat returnStat = keeper.exists(path, wrapWatcher(watcher));
    if (log.isDebugEnabled()) {
      log.debug("exists(String, Watcher, boolean) - end");
    }
    return returnStat;

  }

  /**
   * Returns true if path exists
   */
  public Boolean exists(final String path, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("exists(String path={}, boolean retryOnConnLoss={}) - start", path, retryOnConnLoss);
    }

    Boolean returnBoolean = keeper.exists(path, null) != null;
    if (log.isDebugEnabled()) {
      log.debug("exists(String, boolean) - end");
    }
    return returnBoolean;
  }

  /**
   * Returns children of the node at the path
   */
  public List<String> getChildren(final String path, final Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getChildren(String path={}, Watcher watcher={}, boolean retryOnConnLoss={}) - start", path, watcher,
          retryOnConnLoss);
    }

    List<String> returnList = keeper.getChildren(path, wrapWatcher(watcher));
    if (log.isDebugEnabled()) {
      log.debug("getChildren(String, Watcher, boolean) - end");
    }
    return returnList;

  }

  /**
   * Returns node's data
   */
  public byte[] getData(final String path, final Watcher watcher, final Stat stat, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("getData(String path={}, Watcher watcher={}, Stat stat={}, boolean retryOnConnLoss={}) - start", path,
          watcher, stat, retryOnConnLoss);
    }

    byte[] returnbyteArray = keeper.getData(path, wrapWatcher(watcher), stat);
    if (log.isDebugEnabled()) {
      log.debug("getData(String, Watcher, Stat, boolean) - end");
    }
    return returnbyteArray;

  }

  /**
   * Returns node's state
   */
  public Stat setData(final String path, final byte data[], final int version, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("setData(String path={}, byte[] data={}, int version={}, boolean retryOnConnLoss={}) - start", path,
          data != null ? data.length : "null", version, retryOnConnLoss);
    }

    Stat returnStat = keeper.setData(path, data, version);
    if (log.isDebugEnabled()) {
      log.debug("setData(String, byte[], int, boolean) - end");
    }
    return returnStat;

  }

  public void atomicUpdate(String path, Function<byte[], byte[]> editor) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("atomicUpdate(String path={}, Function<byte[],byte[]> editor={}) - start", path, editor);
    }

   atomicUpdate(path, (stat, bytes) -> editor.apply(bytes));

    if (log.isDebugEnabled()) {
      log.debug("atomicUpdate(String, Function<byte[],byte[]>) - end");
    }
  }

  public void atomicUpdate(String path, BiFunction<Stat , byte[], byte[]> editor) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("atomicUpdate(String path={}, BiFunction<Stat,byte[],byte[]> editor={}) - start", path, editor);
    }

    for (; ; ) {
      byte[] modified = null;
      byte[] zkData = null;
      Stat s = new Stat();
      try {
        if (exists(path, true)) {
          zkData = getData(path, null, s, true);
          modified = editor.apply(s, zkData);
          if (modified == null) {
            //no change , no need to persist

            if (log.isDebugEnabled()) {
              log.debug("atomicUpdate(String, BiFunction<Stat,byte[],byte[]>) - end");
            }
            return;
          }
          setData(path, modified, s.getVersion(), true);
          break;
        } else {
          modified = editor.apply(s,null);
          if (modified == null) {
            //no change , no need to persist

            if (log.isDebugEnabled()) {
              log.debug("atomicUpdate(String, BiFunction<Stat,byte[],byte[]>) - end");
            }
            return;
          }
          create(path, modified, CreateMode.PERSISTENT, true);
          break;
        }
      } catch (KeeperException.BadVersionException | KeeperException.NodeExistsException e) {
        log.error("atomicUpdate(String=" + path + ", BiFunction<Stat,byte[],byte[]>=" + editor + ")", e);

        continue;
      }
    }


    if (log.isDebugEnabled()) {
      log.debug("atomicUpdate(String, BiFunction<Stat,byte[],byte[]>) - end");
    }
  }

  /**
   * Returns path of created node
   */
  public String create(final String path, final byte[] data,
      final CreateMode createMode, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("create(String path={}, byte[] data={}, CreateMode createMode={}, boolean retryOnConnLoss={}) - start",
          path, data != null ? data.length : "null", createMode, retryOnConnLoss);
    }

    List<ACL> acls = zkACLProvider.getACLsToAdd(path);
    String returnString = keeper.create(path, data, acls, createMode);
    if (log.isDebugEnabled()) {
      log.debug("create(String, byte[], CreateMode, boolean) - end");
    }
    return returnString;

  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   */
  public void makePath(String path, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  public void makePath(String path, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, File file, boolean failOnExists, boolean retryOnConnLoss)
      throws IOException, KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file),
        CreateMode.PERSISTENT, null, failOnExists, retryOnConnLoss, 0);
  }

  public void makePath(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    makePath(path, FileUtils.readFileToByteArray(file), retryOnConnLoss);
  }

  public void makePath(String path, CreateMode createMode, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, null, createMode, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null, retryOnConnLoss);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, true, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * @param data to set on the last zkNode
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss) throws KeeperException, InterruptedException {
    makePath(path, data, createMode, watcher, failOnExists, retryOnConnLoss, 0);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   *
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   *
   * skipPathParts will force the call to fail if the first skipPathParts do not exist already.
   *
   * Note: retryOnConnLoss is only respected for the final node - nodes
   * before that are always retried on connection loss.
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher, boolean failOnExists, boolean retryOnConnLoss, int skipPathParts) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("makePath(String path={}, byte[] data={}, CreateMode createMode={}, Watcher watcher={}, boolean failOnExists={}, boolean retryOnConnLoss={}, int skipPathParts={}) - start", path, data != null ? data.length : "null", createMode, watcher, failOnExists, retryOnConnLoss, skipPathParts);
    }

    boolean retry = true;

    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    } else {
      throw new IllegalArgumentException("znode paths must start with /");
    }
    String[] paths = path.split("\r\n|\r|\n");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      String pathPiece = paths[i];
      sbPath.append("/").append(pathPiece);
      if (i < skipPathParts) {
        continue;
      }
      byte[] bytes = null;
      final String currentPath = sbPath.toString();

      CreateMode mode = CreateMode.PERSISTENT;
      if (i == paths.length - 1) {
        mode = createMode;
        bytes = data;
        if (!retryOnConnLoss) retry = false;
      }
      try {
        keeper.create(currentPath, bytes, zkACLProvider.getACLsToAdd(currentPath), mode);
      } catch (NoAuthException e) {
        log.error("makePath(String=" + path + ", byte[]=" + (data != null ? data.length : "null") + ", CreateMode=" + createMode + ", Watcher=" + watcher + ", boolean=" + failOnExists + ", boolean=" + retryOnConnLoss + ", int=" + skipPathParts + ")", e);

        // in auth cases, we may not have permission for an earlier part of a path, which is fine
        if (i == paths.length - 1 || !exists(currentPath, retryOnConnLoss)) {

          throw e;
        }
      } catch (NodeExistsException e) {
        log.error("makePath(String=" + path + ", byte[]=" + (data != null ? data.length : "null") + ", CreateMode=" + createMode + ", Watcher=" + watcher + ", boolean=" + failOnExists + ", boolean=" + retryOnConnLoss + ", int=" + skipPathParts + ")", e);

        if (!failOnExists && i == paths.length - 1) {
          // TODO: version ? for now, don't worry about race
          setData(currentPath, data, -1, retryOnConnLoss);
          // set new watch
          exists(currentPath, watcher, retryOnConnLoss);

          if (log.isDebugEnabled()) {
            log.debug("makePath(String, byte[], CreateMode, Watcher, boolean, boolean, int) - end");
          }
          return;
        }

        // ignore unless it's the last node in the path
        if (i == paths.length - 1) {
          throw e;
        }
      }

    }

    if (log.isDebugEnabled()) {
      log.debug("makePath(String, byte[], CreateMode, Watcher, boolean, boolean, int) - end");
    }
  }

  public void makePath(String zkPath, CreateMode createMode, Watcher watcher, boolean retryOnConnLoss)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher, retryOnConnLoss);
  }

  /**
   * Write data to ZooKeeper.
   */
  public Stat setData(String path, byte[] data, boolean retryOnConnLoss) throws KeeperException,
      InterruptedException {
    Stat returnStat = setData(path, data, -1, retryOnConnLoss);
    return returnStat;
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   *
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param file path to file to be uploaded
   */
  public Stat setData(String path, File file, boolean retryOnConnLoss) throws IOException,
      KeeperException, InterruptedException {
    byte[] data = FileUtils.readFileToByteArray(file);
    Stat returnStat = setData(path, data, retryOnConnLoss);
    return returnStat;
  }

  public List<OpResult> multi(final Iterable<Op> ops, boolean retryOnConnLoss) throws InterruptedException, KeeperException  {
    List<OpResult> results = keeper.multi(ops);
    
    return results;
  }

  /**
   * Fills string with printout of current ZooKeeper layout.
   */
  public void printLayout(String path, int indent, StringBuilder string) {

    byte[] data;
    Stat stat = new Stat();
    List<String> children;
    try {
      data = getData(path, null, stat, true);

      children = getChildren(path, null, true);
      Collections.sort(children);
    } catch (KeeperException | InterruptedException e1) {
      if (e1 instanceof KeeperException.NoNodeException) {
        // things change ...
        return;
      }
      throw new DW.Exp(e1);
    }
    StringBuilder dent = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      dent.append(" ");
    }
    string.append(dent).append(path).append(" (c=").append(children.size()).append(",v=" + (stat == null ? "?" : stat.getVersion()) + ")").append(NEWL);
    if (data != null) {
      String dataString = new String(data, StandardCharsets.UTF_8);
      if ((stat != null && stat.getDataLength() < MAX_BYTES_FOR_ZK_LAYOUT_DATA_SHOW && dataString.split("\\r\\n|\\r|\\n").length < 6) || path.endsWith("state.json")) {
        if (path.endsWith(".xml")) {
          // this is the cluster state in xml format - lets pretty print
          dataString = prettyPrint(path, dataString);
        }

        string.append(dent).append("DATA (" + (stat != null ? stat.getDataLength() : "?") + "b) :\n").append(dent).append("    ")
            .append(dataString.replaceAll("\n", "\n" + dent + "    ")).append(NEWL);
      } else {
        string.append(dent).append("DATA (" + (stat != null ? stat.getDataLength() : "?") + "b) : ...supressed...").append(NEWL);
      }
    }
    indent += 1;
    for (String child : children) {
      if (!child.equals("quota")) {
        printLayout(path + (path.equals("/") ? "" : "/") + child, indent,
            string);
      }
    }
  }

  public void printLayout() {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    log.warn("\n\n_____________________________________________________________________\n\n\nZOOKEEPER LAYOUT:\n\n" + sb.toString() + "\n\n_____________________________________________________________________\n\n");
  }
  
  public void printLayoutToStream(PrintStream out) {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    out.println(sb.toString());
  }
  
  public void printLayoutToFile(Path file) {
    StringBuilder sb = new StringBuilder();
    printLayout("/", 0, sb);
    try {
      Files.writeString(file, sb.toString(), StandardOpenOption.CREATE);
    } catch (IOException e) {
      throw new DW.Exp(e);
    }
  }
  
  public static String prettyPrint(String path, String dataString, int indent) {
    try {
      Source xmlInput = new StreamSource(new StringReader(dataString));
      try (StringWriter stringWriter = new StringWriter()) {
        StreamResult xmlOutput = new StreamResult(stringWriter);
        try (Writer writer = xmlOutput.getWriter()) {
          return writer.toString();
        }
      } finally {
        DW.close(((StreamSource) xmlInput).getInputStream());
      }
    } catch (Exception e) {
      log.error("prettyPrint(path={}, dataString={})", dataString, indent, e);

      DW.propegateInterrupt(e);
      return "XML Parsing Failure";
    }
  }

  private static String prettyPrint(String path, String input) {
    String returnString = prettyPrint(path, input, 2);
    return returnString;
  }

  public void close() {
    if (log.isDebugEnabled()) {
      log.debug("close() - start");
    }

    if (isClosed) return; // it's okay if we over close - same as solrcore
    isClosed = true;
    
    try (DW worker = new DW(this, true)) {
      worker.add("ZkClientExecutors&ConnMgr", connManager, zkCallbackExecutor, zkConnManagerCallbackExecutor, curator);
    }

    assert ObjectReleaseTracker.release(this);

    if (log.isDebugEnabled()) {
      log.debug("close() - end");
    }
  }

  public boolean isClosed() {
    if (log.isDebugEnabled()) {
      log.debug("isClosed() - start");
    }

    boolean returnboolean = isClosed || (higherLevelIsClosed != null && higherLevelIsClosed.isClosed());
    if (log.isDebugEnabled()) {
      log.debug("isClosed() - end");
    }
    return returnboolean;
  }

  /**
   * Allows package private classes to update volatile ZooKeeper.
   */
  void updateKeeper(ZooKeeper keeper) throws InterruptedException {
//    if (log.isDebugEnabled()) {
//      log.debug("updateKeeper(SolrZooKeeper keeper={}) - start", keeper);
//    }
//
//   ZooKeeper oldKeeper = this.keeper;
//   this.keeper = keeper;
//   if (oldKeeper != null) {
//     oldKeeper.close();
//   }
//   // we might have been closed already
//   if (isClosed) this.keeper.close();
//
//    if (log.isDebugEnabled()) {
//      log.debug("updateKeeper(SolrZooKeeper) - end");
//    }
  }

  public ZooKeeper getSolrZooKeeper() {
    return keeper;
  }

  private void closeKeeper(ZooKeeper keeper) {
    if (log.isDebugEnabled()) {
      log.debug("closeKeeper(SolrZooKeeper keeper={}) - start", keeper);
    }

    if (keeper != null) {
      try {
        keeper.close();
      } catch (InterruptedException e) {
        log.error("closeKeeper(SolrZooKeeper=" + keeper + ")", e);

        throw new DW.Exp(e);
      }
    }

    if (log.isDebugEnabled()) {
      log.debug("closeKeeper(SolrZooKeeper) - end");
    }
  }

  private void closeCallbackExecutor() {
    if (log.isDebugEnabled()) {
      log.debug("closeCallbackExecutor() - start");
    }

    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkCallbackExecutor);
    } catch (Exception e) {
      log.error("closeCallbackExecutor()", e);

      throw new DW.Exp(e);
    }

    try {
      ExecutorUtil.shutdownAndAwaitTermination(zkConnManagerCallbackExecutor);
    } catch (Exception e) {
      log.error("closeCallbackExecutor()", e);

      throw new DW.Exp(e);
    }

    if (log.isDebugEnabled()) {
      log.debug("closeCallbackExecutor() - end");
    }
  }


  /**
   * Validates if zkHost contains a chroot. See http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
   */
  public static boolean containsChroot(String zkHost) {
    if (log.isDebugEnabled()) {
      log.debug("containsChroot(String zkHost={}) - start", zkHost);
    }

    boolean returnboolean = zkHost.contains("/");
    if (log.isDebugEnabled()) {
      log.debug("containsChroot(String) - end");
    }
    return returnboolean;
  }

  /**
   * Check to see if a Throwable is an InterruptedException, and if it is, set the thread interrupt flag
   * @param e the Throwable
   * @return the Throwable
   */
  public static Throwable checkInterrupted(Throwable e) {
    if (log.isDebugEnabled()) {
      log.debug("checkInterrupted(Throwable e={}) - start", e);
    }

    if (e instanceof InterruptedException)
      Thread.currentThread().interrupt();

    if (log.isDebugEnabled()) {
      log.debug("checkInterrupted(Throwable) - end");
    }
    return e;
  }

  /**
   * @return the address of the zookeeper cluster
   */
  public String getZkServerAddress() {
    return zkServerAddress;
  }

  public ZkACLProvider getZkACLProvider() {
    return zkACLProvider;
  }

  /**
   * Set the ACL on a single node in ZooKeeper. This will replace all existing ACL on that node.
   *
   * @param path path to set ACL on e.g. /solr/conf/solrconfig.xml
   * @param acls a list of {@link ACL}s to be applied
   * @param retryOnConnLoss true if the command should be retried on connection loss
   */
  public Stat setACL(String path, List<ACL> acls, boolean retryOnConnLoss)
      throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) {
      log.debug("setACL(String path={}, List<ACL> acls={}, boolean retryOnConnLoss={}) - start", path, acls,
          retryOnConnLoss);
    }

    Stat returnStat = keeper.setACL(path, acls, -1);
    if (log.isDebugEnabled()) {
      log.debug("setACL(String, List<ACL>, boolean) - end");
    }
    return returnStat;

  }

  /**
   * Update all ACLs for a zk tree based on our configured {@link ZkACLProvider}.
   * @param root the root node to recursively update
   */
  public void updateACLs(final String root) throws KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("updateACLs(String root={}) - start", root);
    }

    ZkMaintenanceUtils.traverseZkTree(this, root, ZkMaintenanceUtils.VISIT_ORDER.VISIT_POST, path -> {
      try {
        setACL(path, getZkACLProvider().getACLsToAdd(path), true);
        //if (log.isDebugEnabled()) log.debug("Updated ACL on {}", path);
      } catch (NoNodeException e) {
        // If a node was deleted, don't bother trying to set ACLs on it.
        return;
      }
    });

    if (log.isDebugEnabled()) {
      log.debug("updateACLs(String) - end");
    }
  }

  // Some pass-throughs to allow less code disruption to other classes that use SolrZkClient.
  public void clean(String path) throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) {
      log.debug("clean(String path={}) - start", path);
    }

    ZkMaintenanceUtils.clean(this, path);

    if (log.isDebugEnabled()) {
      log.debug("clean(String) - end");
    }
  }

  public void clean(String path, Predicate<String> nodeFilter) throws InterruptedException, KeeperException {
    if (log.isDebugEnabled()) {
      log.debug("clean(String path={}, Predicate<String> nodeFilter={}) - start", path, nodeFilter);
    }

    ZkMaintenanceUtils.clean(this, path, nodeFilter);

    if (log.isDebugEnabled()) {
      log.debug("clean(String, Predicate<String>) - end");
    }
  }

  public void upConfig(Path confPath, String confName) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("upConfig(Path confPath={}, String confName={}) - start", confPath, confName);
    }

    ZkMaintenanceUtils.upConfig(this, confPath, confName);

    if (log.isDebugEnabled()) {
      log.debug("upConfig(Path, String) - end");
    }
  }

  public String listZnode(String path, Boolean recurse) throws KeeperException, InterruptedException, SolrServerException {
    if (log.isDebugEnabled()) {
      log.debug("listZnode(String path={}, Boolean recurse={}) - start", path, recurse);
    }

    String returnString = ZkMaintenanceUtils.listZnode(this, path, recurse);
    if (log.isDebugEnabled()) {
      log.debug("listZnode(String, Boolean) - end");
    }
    return returnString;
  }

  public void downConfig(String confName, Path confPath) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("downConfig(String confName={}, Path confPath={}) - start", confName, confPath);
    }

    ZkMaintenanceUtils.downConfig(this, confName, confPath);

    if (log.isDebugEnabled()) {
      log.debug("downConfig(String, Path) - end");
    }
  }

  public void zkTransfer(String src, Boolean srcIsZk,
                         String dst, Boolean dstIsZk,
                         Boolean recurse) throws SolrServerException, KeeperException, InterruptedException, IOException {
    if (log.isDebugEnabled()) {
      log.debug("zkTransfer(String src={}, Boolean srcIsZk={}, String dst={}, Boolean dstIsZk={}, Boolean recurse={}) - start", src, srcIsZk, dst, dstIsZk, recurse);
    }

    ZkMaintenanceUtils.zkTransfer(this, src, srcIsZk, dst, dstIsZk, recurse);

    if (log.isDebugEnabled()) {
      log.debug("zkTransfer(String, Boolean, String, Boolean, Boolean) - end");
    }
  }

  public void moveZnode(String src, String dst) throws SolrServerException, KeeperException, InterruptedException {
    if (log.isDebugEnabled()) {
      log.debug("moveZnode(String src={}, String dst={}) - start", src, dst);
    }

    ZkMaintenanceUtils.moveZnode(this, src, dst);

    if (log.isDebugEnabled()) {
      log.debug("moveZnode(String, String) - end");
    }
  }

  public void uploadToZK(final Path rootPath, final String zkPath,
                         final Pattern filenameExclusions) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("uploadToZK(Path rootPath={}, String zkPath={}, Pattern filenameExclusions={}) - start", rootPath, zkPath, filenameExclusions);
    }

    ZkMaintenanceUtils.uploadToZK(this, rootPath, zkPath, filenameExclusions);

    if (log.isDebugEnabled()) {
      log.debug("uploadToZK(Path, String, Pattern) - end");
    }
  }
  public void downloadFromZK(String zkPath, Path dir) throws IOException {
    if (log.isDebugEnabled()) {
      log.debug("downloadFromZK(String zkPath={}, Path dir={}) - start", zkPath, dir);
    }

    ZkMaintenanceUtils.downloadFromZK(this, zkPath, dir);

    if (log.isDebugEnabled()) {
      log.debug("downloadFromZK(String, Path) - end");
    }
  }

  /**
   * Watcher wrapper that ensures that heavy implementations of process do not interfere with our ability
   * to react to other watches, but also ensures that two wrappers containing equal watches are considered
   * equal (and thus we won't accumulate multiple wrappers of the same watch).
   */
  private final class ProcessWatchWithExecutor implements Watcher { // see below for why final.
    private final Watcher watcher;

    ProcessWatchWithExecutor(Watcher watcher) {
      if (watcher == null) {
        throw new IllegalArgumentException("Watcher must not be null");
      }
      this.watcher = watcher;
    }

    @Override
    public void process(final WatchedEvent event) {
      if (log.isDebugEnabled()) {
        log.debug("process(WatchedEvent event={}) - start", event);
      }

     // log.debug("Submitting job to respond to event {}", event);
      try {
        if (watcher instanceof ConnectionManager) {
          zkConnManagerCallbackExecutor.submit(() -> watcher.process(event));
        } else {
          zkCallbackExecutor.submit(() -> watcher.process(event));
        }
      } catch (RejectedExecutionException e) {
        log.error("process(WatchedEvent=" + event + ")", e);

        // If not a graceful shutdown
        if (!isClosed()) {
          throw e;
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("process(WatchedEvent) - end");
      }
    }

    // These overrides of hashcode/equals ensure that we don't store the same exact watch
    // multiple times in org.apache.zookeeper.ZooKeeper.ZKWatchManager.dataWatches
    // (a Map<String<Set<Watch>>). This class is marked final to avoid oddball
    // cases with sub-classes, if you need different behavior, find a new class or make
    // sure you account for the case where two diff sub-classes with different behavior
    // for process(WatchEvent) and have been created with the same watch object.
    @Override
    public int hashCode() {
      return watcher.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ProcessWatchWithExecutor) {
        return this.watcher.equals(((ProcessWatchWithExecutor) obj).watcher);
      }
      return false;
    }
  }

  public void mkdirs(String path) {
    Map<String,byte[]> dataMap = new HashMap<String,byte[]>(1);
    dataMap.put(path, null);
    mkDirs(dataMap);
  }
  
  public void mkdirs(String path, byte[] bytes) {
    Map<String,byte[]> dataMap = new HashMap<String,byte[]>(1);
    dataMap.put(path, bytes);
    mkDirs(dataMap);
  }
  
  public void mkDirs(String... paths) {
    Map<String,byte[]> dataMap = new HashMap<String,byte[]>(paths.length);
    for (String path : paths) {
      dataMap.put(path, null);
    }
    mkDirs(dataMap);
  }

  public void mkDirs( Map<String,byte[]> dataMap) {
    Set<String> paths = dataMap.keySet();
    
    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String paths={}) - start", paths);
    }
    
    List<String> pathsToMake = new ArrayList<>(paths.size() * 3);
 
    for (String fullpath : paths) {
      if (!fullpath.startsWith("/")) throw new IllegalArgumentException("Paths must start with /");
      StringBuilder sb = new StringBuilder();
      if (log.isDebugEnabled()) {
        log.debug("path {}", fullpath);
      }
      String[] subpaths = fullpath.split("/");
      for (String subpath : subpaths) {
        if (subpath.length() == 0) continue;
        if (log.isDebugEnabled()) {
          log.debug("subpath {}", subpath);
        }
        sb.append("/" + subpath.replaceAll("\\/", ""));
        pathsToMake.add(sb.toString());
      }
    }

    CountDownLatch latch = new CountDownLatch(pathsToMake.size());
    int[] code = new int[1];
    String[] contextArray = new String[1];
    for (String makePath : pathsToMake) {

      if (!makePath.startsWith("/")) makePath = "/" + makePath;
      
      byte[] data = dataMap.get(makePath);
      
      if (log.isDebugEnabled()) {
        log.debug("makepath {}", makePath + " data: " + (data == null ? "none" : data.length + "b"));
      }
      assert getZkACLProvider() != null;
      assert keeper != null;
      keeper.create(makePath, data, getZkACLProvider().getACLsToAdd(makePath), CreateMode.PERSISTENT,
          (resultCode, path, context, name) -> {
            code[0] = resultCode;
            contextArray[0] = "" + context;
            latch.countDown();
          }, "");

    }
    boolean success = false;
    try {
      success = latch.await(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("mkDirs(String=" + paths + ")", e);

      throw new DW.Exp(e);
    }
    
    if (!success) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Timeout waiting for operatoins to complete");
    }

    if (log.isDebugEnabled()) {
      log.debug("mkDirs(String) - end");
    }
  }

  public void mkdirs(String znode, File file) {
    try {
      mkdirs(znode, FileUtils.readFileToByteArray(file));
    } catch (Exception e) {
      throw new DW.Exp(e);
    }
  }

  public CuratorFramework getCurator() {
    return curator;
  }
  
  public AsyncCuratorFramework getAsynCurator() {
    return asyncCurator;
  }
  
  public static void main(String[] args) {
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework client = CuratorFrameworkFactory.newClient(args[0], retryPolicy);
    client.start();
    try {
      client.blockUntilConnected(10000, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      DW.propegateInterrupt(e);
    }

    try (SolrZkClient zkClient = new SolrZkClient(client)) {
      zkClient.printLayoutToStream(System.out);
    }
    
  }
}
