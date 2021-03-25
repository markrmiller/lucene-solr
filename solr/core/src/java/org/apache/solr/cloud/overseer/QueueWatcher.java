package org.apache.solr.cloud.overseer;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.core.CoreContainer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public abstract class QueueWatcher implements Watcher, Closeable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final CoreContainer cc;
  protected final ZkController zkController;
  protected final String path;
  protected final Overseer overseer;
  protected volatile List<String> startItems;
  protected volatile boolean closed;
  protected final ReentrantLock ourLock = new ReentrantLock(false);

  public QueueWatcher(CoreContainer cc, Overseer overseer, String path) throws KeeperException {
    this.cc = cc;
    this.zkController = cc.getZkController();
    this.overseer = overseer;
    this.path = path;
  }

  public abstract void start(boolean weAreReplacement) throws KeeperException, InterruptedException;

  public abstract void process(WatchedEvent event);

  protected abstract void processQueueItems(List<String> items, boolean onStart, boolean weAreReplacement);

  protected abstract void doneWithCurrentQueue();

  @Override
  public void close() {
    this.closed = true;
    closeWatcher();
  }

  private void closeWatcher() {
    try {
      zkController.getZkClient().removeAllWatches(path);
    } catch (KeeperException.NoWatcherException | AlreadyClosedException e) {

    } catch (Exception e) {
      log.info("could not remove watch {} {}", e.getClass().getSimpleName(), e.getMessage());
    }
  }
}
