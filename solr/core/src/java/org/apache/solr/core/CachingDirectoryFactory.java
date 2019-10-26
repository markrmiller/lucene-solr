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
package org.apache.solr.core;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.patterns.DW;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.TimeOut;
import org.apache.solr.common.util.TimeSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String, LockFactory, DirContext)}.
 * <p>
 * This is an expert class and these API's are subject to change.
 */
// nocommit parrallize close
public abstract class CachingDirectoryFactory extends DirectoryFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  protected static class CacheValue {
    final public String path;
    final public Directory directory;
    // for debug
    //final Exception originTrace;
    // use the setter!
    private volatile boolean deleteOnClose = false;

    public CacheValue(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
      this.closeEntries.add(this);
      // for debug
      // this.originTrace = new RuntimeException("Originated from:");
    }

    public AtomicInteger refCnt = new AtomicInteger(1);
    // has doneWithDirectory(Directory) been called on this?
    public volatile boolean closeCacheValueCalled = false;
    public volatile boolean doneWithDir = false;
    private volatile boolean deleteAfterCoreClose = false;
    public Set<CacheValue> removeEntries = ConcurrentHashMap.newKeySet(1000);
    public Set<CacheValue> closeEntries = ConcurrentHashMap.newKeySet(1000);

    public void setDeleteOnClose(boolean deleteOnClose, boolean deleteAfterCoreClose) {
      if (deleteOnClose) {
        removeEntries.add(this);
      }
      this.deleteOnClose = deleteOnClose;
      this.deleteAfterCoreClose = deleteAfterCoreClose;
    }

    @Override
    public String toString() {
      return "CachedDir<<" + "refCount=" + refCnt + ";path=" + path + ";done=" + doneWithDir + ">>";
    }
  }

  protected Map<String, CacheValue> byPathCache = new ConcurrentHashMap<>(1000, 0.75f, 100);

  protected Map<Directory, CacheValue> byDirectoryCache = new ConcurrentHashMap<>(1000, 0.75f, 100);

  protected Map<Directory, List<CloseListener>> closeListeners = new ConcurrentHashMap<>(1000, 0.75f, 100);

  protected Set<CacheValue> removeEntries = ConcurrentHashMap.newKeySet(1000);

  private volatile boolean closed;

  public interface CloseListener {
    public void postClose();

    public void preClose();
  }

  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {
    if (!byDirectoryCache.containsKey(dir)) {
      throw new IllegalArgumentException("Unknown directory: " + dir
          + " " + byDirectoryCache);
    }
    
    synchronized (closeListeners) {
      List<CloseListener> listeners = closeListeners.get(dir);
      if (listeners == null) {
        listeners = new ArrayList<>();
        closeListeners.put(dir, listeners);
      }
      listeners.add(closeListener);

      closeListeners.put(dir, listeners);
    }

  }

  @Override
  public void doneWithDirectory(Directory directory) throws IOException {
    CacheValue cacheValue = byDirectoryCache.get(directory);
    if (cacheValue == null) {
      throw new IllegalArgumentException("Unknown directory: " + directory
          + " " + byDirectoryCache);
    }
    cacheValue.doneWithDir = true;
    log.debug("Done with dir: {}", cacheValue);
    if (cacheValue.refCnt.get() == 0 && !closed) {
      boolean cl = closeCacheValue(cacheValue);
      if (cl) {
        removeFromCache(cacheValue);
      }
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
      this.closed = true;
      log.debug("Closing {} - {} directories currently being tracked", this.getClass().getSimpleName(), byDirectoryCache.size());
      TimeOut timeout = new TimeOut(15, TimeUnit.SECONDS,  TimeSource.NANO_TIME);
      this.closed = true;
      Collection<CacheValue> values = byDirectoryCache.values();
      for (CacheValue val : values) {
        log.debug("Closing {} - currently tracking: {}",
            this.getClass().getSimpleName(), val);
        try {
          // if there are still refs out, we have to wait for them
          assert val.refCnt.get()  > -1 : val.refCnt;
          while (val.refCnt.get()  != 0) {
            wait(300);

            if (timeout.hasTimedOut()) {
              String msg = "Timeout waiting for all directory ref counts to be released - gave up waiting on " + val;
              log.error(msg);
              // debug
              // val.originTrace.printStackTrace();
              throw new SolrException(ErrorCode.SERVER_ERROR, msg);
            }
          }
          assert val.refCnt.get()  == 0 : val.refCnt;
        } catch (Exception e) {
          throw new DW.Exp("Error closing directory", e);
        }
      }

      values = byDirectoryCache.values();
      Set<CacheValue> closedDirs = new HashSet<>();
      for (CacheValue val : values) {
        try {
          for (CacheValue v : val.closeEntries) {
            assert v.refCnt.get()  == 0 : val.refCnt;
            log.debug("Closing directory when closing factory: " + v.path);
            boolean cl = false;
            try {
              cl = closeCacheValue(v);
            } catch (Throwable e) {
              throw new DW.Exp("Error closing directory", e);
            }
            if (cl) {
              closedDirs.add(v);
            }
          }
        } catch (Exception e) {
          throw new DW.Exp("Error closing directory", e);
        }
      }

      for (CacheValue val : removeEntries) {
        log.debug("Removing directory after core close: " + val.path);
        try {
          removeDirectory(val);
        } catch (Exception e) {
          throw new DW.Exp("Error removing directory", e);
        }
      }

      for (CacheValue v : closedDirs) {
        removeFromCache(v);
      }
    
  }

  private void removeFromCache(CacheValue v) {
    log.debug("Removing from cache: {}", v);
    byDirectoryCache.remove(v.directory);
    byPathCache.remove(v.path);
  }

  // be sure this is called with the this sync lock
  // returns true if we closed the cacheValue, false if it will be closed later
  private boolean closeCacheValue(CacheValue cacheValue) {
    log.debug("looking to close {} {}", cacheValue.path, cacheValue.closeEntries.toString());
    List<CloseListener> listeners = null;
    try {
      listeners = closeListeners.remove(cacheValue.directory);
      if (listeners != null) {
        for (CloseListener listener : listeners) {
          try {
            listener.preClose();
          } catch (Exception e) {
            throw new DW.Exp(e);
          }
        }
      }
      cacheValue.closeCacheValueCalled = true;
      if (cacheValue.deleteOnClose) {
        // see if we are a subpath
        Collection<CacheValue> values = byPathCache.values();

        Collection<CacheValue> cacheValues = new ArrayList<>(values);
        cacheValues.remove(cacheValue);
        for (CacheValue otherCacheValue : cacheValues) {
          // if we are a parent path and a sub path is not already closed, get a sub path to close us later
          if (isSubPath(cacheValue, otherCacheValue) && !otherCacheValue.closeCacheValueCalled) {
            // we let the sub dir remove and close us
            if (!otherCacheValue.deleteAfterCoreClose && cacheValue.deleteAfterCoreClose) {
              otherCacheValue.deleteAfterCoreClose = true;
            }
            otherCacheValue.removeEntries.addAll(cacheValue.removeEntries);
            otherCacheValue.closeEntries.addAll(cacheValue.closeEntries);
            cacheValue.closeEntries.clear();
            cacheValue.removeEntries.clear();
            return false;
          }
        }
      }

    } catch (Exception e) {
      throw new DW.Exp("Exception releasing directory", e);
    }

    boolean cl = false;
    for (CacheValue val : cacheValue.closeEntries) {
      try {
        close(val);
      } catch (Exception e) {
        throw new DW.Exp("Exception closing", e);
      }
      if (val == cacheValue) {
        cl = true;
      }
    }

    try {
      for (CacheValue val : cacheValue.removeEntries) {
        if (!val.deleteAfterCoreClose) {
          log.debug("Removing directory before core close: " + val.path);
          try {
            removeDirectory(val);
          } catch (Exception e) {
            throw new DW.Exp("Error removing directory " + val.path + " before core close", e);
          }
        } else {
          removeEntries.add(val);
        }
      }
    } catch (Exception e) {
      throw new DW.Exp("Exception releasing directory", e);
    }

    if (listeners != null) {
      for (CloseListener listener : listeners) {
        try {
          listener.postClose();
        } catch (Exception e) {
          throw new DW.Exp("Error executing postClose for directory", e);
        }
      }
    }
    return cl;
  }

  private void close(CacheValue val) {
    log.debug("Closing directory, CoreContainer#isShutdown={}", coreContainer != null ? coreContainer.isShutDown() : "null");
    try {
      if (coreContainer != null && coreContainer.isShutDown() && val.directory instanceof ShutdownAwareDirectory) {
        log.debug("Closing directory on shutdown: " + val.path);
        ((ShutdownAwareDirectory) val.directory).closeOnShutdown();
      } else {
        log.debug("Closing directory: " + val.path);
        val.directory.close();
      }
      assert ObjectReleaseTracker.release(val.directory);
    } catch (Exception e) {
      throw new DW.Exp("Error closing directory", e);
    }
  }

  private boolean isSubPath(CacheValue cacheValue, CacheValue otherCacheValue) {
    int one = cacheValue.path.lastIndexOf('/');
    int two = otherCacheValue.path.lastIndexOf('/');

    return otherCacheValue.path.startsWith(cacheValue.path + "/") && two > one;
  }

  @Override
  public boolean exists(String path) throws IOException {
    // back compat behavior
    File dirFile = new File(path);
    return dirFile.canRead() && dirFile.list().length > 0;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path, DirContext dirContext, String rawLockType)
      throws IOException {
    String fullPath = normalize(path);

      if (closed) {
        throw new AlreadyClosedException("Already closed");
      }

      final CacheValue cacheValue = byPathCache.get(fullPath);
      Directory directory = null;
      if (cacheValue != null) {
        directory = cacheValue.directory;
      }

      if (directory == null) {
        directory = create(fullPath, createLockFactory(rawLockType), dirContext);
        assert ObjectReleaseTracker.track(directory);
        boolean success = false;
        try {
          CacheValue newCacheValue = new CacheValue(fullPath, directory);
          byDirectoryCache.put(directory, newCacheValue);
          byPathCache.put(fullPath, newCacheValue);
          log.info("return new directory for {}", newCacheValue);
          success = true;
        } finally {
          if (!success) {
            DW.close(directory);
          }
        }
      } else {
        cacheValue.refCnt.incrementAndGet();
        log.info("Reusing cached directory: {}", cacheValue);
      }

      return directory;
    
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.solr.core.DirectoryFactory#incRef(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void incRef(Directory directory) {
    CacheValue cacheValue = byDirectoryCache.get(directory);
    if (cacheValue == null) {
      throw new IllegalArgumentException("Unknown directory: " + directory);
    }

    cacheValue.refCnt.incrementAndGet();
    if (log.isDebugEnabled()) log.debug("incRef'ed: {}", cacheValue);
  }

  @Override
  public void init(NamedList args) {
    // override global config
    if (args.get(SolrXmlConfig.SOLR_DATA_HOME) != null) {
      dataHomePath = Paths.get((String) args.get(SolrXmlConfig.SOLR_DATA_HOME));
    }
    if (dataHomePath != null) {
      log.info(SolrXmlConfig.SOLR_DATA_HOME + "=" + dataHomePath);
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.solr.core.DirectoryFactory#release(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void release(Directory directory) throws IOException {
    if (directory == null) {
      NullPointerException e = new NullPointerException();
      log.error("Attempt to release null directory", e);
      throw e;
    }
    // don't check if already closed here - we need to able to release
    // while #close() waits.

    CacheValue cacheValue = byDirectoryCache.get(directory);
    if (cacheValue == null) {
      log.error("Attempt to release unknown directory: " + directory);
      throw new IllegalArgumentException("Unknown directory: " + directory
          + " " + byDirectoryCache);
    }

    log.info(
        "Releasing directory: " + cacheValue.path + " " + (cacheValue.refCnt.get() - 1) + " " + cacheValue.doneWithDir);

    cacheValue.refCnt.decrementAndGet();

    assert cacheValue.refCnt.get() >= 0 : cacheValue.refCnt;

    if (cacheValue.refCnt.get() == 0 && cacheValue.doneWithDir && !closed) {
      boolean cl = closeCacheValue(cacheValue);
      if (cl) {
        removeFromCache(cacheValue);
      }
    }
  }

  @Override
  public void remove(String path) throws IOException {
    remove(path, false);
  }

  @Override
  public void remove(Directory dir) throws IOException {
    remove(dir, false);
  }

  @Override
  public void remove(String path, boolean deleteAfterCoreClose) throws IOException {
    synchronized (this) {
      CacheValue val = byPathCache.get(normalize(path));
      if (val == null) {
        throw new IllegalArgumentException("Unknown directory " + path);
      }
      val.setDeleteOnClose(true, deleteAfterCoreClose);
    }
  }

  @Override
  public void remove(Directory dir, boolean deleteAfterCoreClose) throws IOException {
    CacheValue val = byDirectoryCache.get(dir);
    if (val == null) {
      throw new IllegalArgumentException("Unknown directory " + dir);
    }
    val.setDeleteOnClose(true, deleteAfterCoreClose);
  }

  protected void removeDirectory(CacheValue cacheValue) throws IOException {
    // this page intentionally left blank
  }

  @Override
  public String normalize(String path) throws IOException {
    path = stripTrailingSlash(path);
    return path;
  }

  protected String stripTrailingSlash(String path) {
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }

  /**
   * Method for inspecting the cache
   *
   * @return paths in the cache which have not been marked "done"
   * @see #doneWithDirectory
   */
  public Set<String> getLivePaths() {
    HashSet<String> livePaths = new HashSet<>();
    byPathCache.forEach((k,v) -> { if (!v.doneWithDir) livePaths.add(v.path);});

    return livePaths;
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    Set<String> livePaths = getLivePaths();
    if (livePaths.contains(oldDirPath)) {
      log.warn("Cannot delete directory {} as it is still being referenced in the cache!", oldDirPath);
      return false;
    }

    return super.deleteOldIndexDirectory(oldDirPath);
  }

  protected String getPath(Directory directory) {
    return byDirectoryCache.get(directory).path;
  }
}
