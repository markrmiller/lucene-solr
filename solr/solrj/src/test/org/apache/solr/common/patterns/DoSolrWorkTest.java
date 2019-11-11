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
package org.apache.solr.common.patterns;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoSolrWorkTest extends SolrTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static class CloseMe implements Closeable, AutoCloseable {
    private final AtomicInteger closes = new AtomicInteger(0);
    volatile boolean closed = false;
    @Override
    public void close() throws IOException {
      log.info("close CloseMe");
      if (closes.incrementAndGet() > 1) throw new RuntimeException("Too many closes");
      this.closed = true;
    }

  }
  
  private static class CloseMe2 implements Closeable {
    private final AtomicInteger closes = new AtomicInteger(0);
    final CloseMe child = new CloseMe();
    
    volatile boolean closed = false;
    @Override
    public void close() throws IOException {
      log.info("close CloseMe2");
      if (closes.incrementAndGet() > 1) throw new RuntimeException("Too many closes");
      this.closed = true;
      child.close();
    }

  }

  @Test
  public void testBasicClose() throws Exception {
    CloseMe closeMe = new CloseMe();
    try (SW worker = new SW(this)) {
      worker.add(closeMe);
    }
    
    assertTrue("CloseMe was not closed!", closeMe.closed);
  }
  
  @Test
  public void testNestedClose() throws Exception {
    CloseMe2 closeMe2 = new CloseMe2();
    try (SW worker = new SW(this)) {
      worker.add(closeMe2);
    }
   
    assertTrue("CloseMe2 was not closed!", closeMe2.closed);
    assertTrue("CloseMe child was not closed!", closeMe2.child.closed);
  }

  @Test(expected = SolrException.class)
  public void testUnknownObject() throws Exception {
    try (SW worker = new SW(this)) {
      worker.add(new Object());
    }
  }
  
  
  @Test
  public void testExecutor() throws Exception {
    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool(3, new DefaultSolrThreadFactory("smartWorkTest"));

    
    try (SW worker = new SW(this)) {
      worker.add(executor);
    }
   
    assertTrue("Executor is not shutdown!", executor.isShutdown());
    assertTrue("Executor is not terminated!", executor.isTerminated());
  }
  
}
