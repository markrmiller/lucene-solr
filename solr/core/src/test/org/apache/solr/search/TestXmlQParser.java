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
package org.apache.solr.search;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.queryparser.xml.CoreParser;
import org.apache.lucene.queryparser.xml.TestCoreParser;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.util.StartupLoggingUtils;
import org.junit.AfterClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;

@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
@ThreadLeakLingering(linger = 0)
public class TestXmlQParser extends TestCoreParser {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private CoreParser solrCoreParser;

  @AfterClass
  public static void shutdownLogger() throws Exception {
    StartupLoggingUtils.shutdown();
  }

  @Override
  protected CoreParser coreParser() {
    if (solrCoreParser == null) {
      solrCoreParser = new SolrCoreParser(
          super.defaultField(),
          super.analyzer(),
          null);
    }
    return solrCoreParser;
  }

  //public void testSomeOtherQuery() {
  //  Query q = parse("SomeOtherQuery.xml");
  //  dumpResults("SomeOtherQuery", q, ?);
  //}

}
