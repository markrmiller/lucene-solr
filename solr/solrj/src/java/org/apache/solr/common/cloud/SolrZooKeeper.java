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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.solr.common.patterns.SW;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

// we use this class to expose nasty stuff for tests
public class SolrZooKeeper extends ZooKeeper {
  final Set<Thread> spawnedThreads = new CopyOnWriteArraySet<>();
  
  // for test debug
  //static Map<SolrZooKeeper,Exception> clients = new ConcurrentHashMap<SolrZooKeeper,Exception>();

  public SolrZooKeeper(String connectString, int sessionTimeout,
      Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    //clients.put(this, new RuntimeException());
  }
  
  public ClientCnxn getConnection() {
    return cnxn;
  }
  
  public ClientCnxn getEventThread() {
    return cnxn;
  }
  
  public SocketAddress getSocketAddress() {
    return testableLocalSocketAddress();
  }
  
  public void closeCnxn() {
    final Thread t = new Thread() {
      @Override
      public void run() {
        AccessController.doPrivileged((PrivilegedAction<Void>) this::closeZookeeperChannel);
      }
      
      @SuppressForbidden(reason = "Hack for Zookeper needs access to private methods.")
      private Void closeZookeeperChannel() {
        final ClientCnxn cnxn = getConnection();
        synchronized (cnxn) {
          try {
            final Field sendThreadFld = cnxn.getClass().getDeclaredField("sendThread");
            sendThreadFld.setAccessible(true);
            Object sendThread = sendThreadFld.get(cnxn);
            if (sendThread != null) {
              Method method = sendThread.getClass().getDeclaredMethod("testableCloseSocket");
              method.setAccessible(true);
              try {
                method.invoke(sendThread);
              } catch (InvocationTargetException e) {
                // is fine
              }
            }
          } catch (Exception e) {
            throw new SW.Exp(e);
          }
        }
        return null; // Void
      }
    };
    spawnedThreads.add(t);
   
    t.start();
  }
  
  public void close() throws InterruptedException {
    
    try (SW worker = new SW(this)) {
      worker.add("SolrZooKeeper", () -> {
        try {
          SolrZooKeeper.super.close();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }); // we don't wait for close because we wait below
      worker.add("InterruptThreads", () -> {
        for (Thread t : spawnedThreads) {
          t.interrupt();
        }
      });
      worker.add("InterruptThreads", () -> {
        call(cnxn, "sendThread", "interrupt", null);
        call(cnxn, "eventThread", "interrupt", null);
        
        call(cnxn, "sendThread", "join", 50l);
        call(cnxn, "eventThread", "join", 50l);
        
        call(cnxn, "sendThread", "interrupt", null);
        call(cnxn, "eventThread", "interrupt", null);
        
        call(cnxn, "sendThread", "join", 1000l);
        call(cnxn, "eventThread", "join", 1000l);
      });
    }
  }


  private void call(final ClientCnxn cnxn, String field, String meth, Object arg) {
    try {
      final Field sendThreadFld = cnxn.getClass().getDeclaredField(field);
      sendThreadFld.setAccessible(true);
      Object sendThread = sendThreadFld.get(cnxn);
      if (sendThread != null) {
        Method method;
        if (arg != null) {
          method = sendThread.getClass().getMethod(meth, long.class);
        } else {
          method = sendThread.getClass().getMethod(meth);
        }
        method.setAccessible(true);
        try {
          if (arg != null) {
            method.invoke(sendThread, arg);
          } else {
            method.invoke(sendThread);
          }
        } catch (InvocationTargetException e) {
          // is fine
        }
      }
    } catch (Exception e) {
      throw new SW.Exp(e);
    }
  }
}
  
