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
package org.apache.solr;

public class TestSysProp {

  /**
   * Solr expects to find a _default config set to work with, but if you test has no need for this config and will not,
   * you can suppress uploading it for a faster test.
   */
  public static void supressDefaultConfigBootstrap() {
    System.setProperty("solr.suppressDefaultConfigBootstrap", "true");
  }
  
  /**
   * The public key handler can be slow in generating things and may block multiple threads, you can suppress adding it
   * for a faster test.
   */
  public static void disablePublicKeyHandler() {
    System.setProperty("solr.disablePublicKeyHandler", "true");
  }
  
  
  /**
   * This is not reccomended, but useful for checking out how much this contributes to performance in some cases.
   */
  public static void disableDisableJvmMetrics() {
    System.setProperty("solr.disableJvmMetrics", "true");
  }


}
