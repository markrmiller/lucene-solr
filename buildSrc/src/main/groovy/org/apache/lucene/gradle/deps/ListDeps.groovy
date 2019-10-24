package org.apache.lucene.gradle.deps
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
import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.bundling.Compression

class ListDeps extends DefaultTask {
  
  @TaskAction
  void list() {
    println "Runtime Deps"
    def runtimeArtifacts = project.configurations.runtimeClasspath.getResolvedConfiguration().getResolvedArtifacts()
    runtimeArtifacts = runtimeArtifacts.toSorted{a,b -> a.name <=> b.name}
    runtimeArtifacts.forEach( { ra -> println ra })
    
    println "-"
    
    println "TestRuntime Deps"
    def testRuntimeArtifacts = project.configurations.testRuntimeClasspath.getResolvedConfiguration().getResolvedArtifacts()
 
    testRuntimeArtifacts = testRuntimeArtifacts.toSorted{a,b -> a.name <=> b.name}
    testRuntimeArtifacts.forEach( { ra -> println ra })
    
    println "-"
    println 'runtime count: ' + runtimeArtifacts.size()
    println 'testRuntime count: ' + testRuntimeArtifacts.size()
  }
}


