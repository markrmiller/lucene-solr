package org.apache.lucene.gradle
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
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

class JFlex extends DefaultTask {
  
  @InputDirectory
  File inputDir
  
  @Input
  String fileName

  @Input
  boolean disableBufferExpansion = false
  
  @OutputDirectory
  File target
  
  public JFlex() {
    dependsOn project.rootProject.project(':buildSrc').tasks.setupAntPaths
  }
  
  @TaskAction
  void jflex() {
    def skeleton
    if (disableBufferExpansion) {
      skeleton = project.project(":lucene:lucene-core").projectDir.getAbsolutePath() + "/src/data/jflex/skeleton.disable.buffer.expansion.txt"
    } else {
      skeleton = project.project(":lucene:lucene-core").projectDir.getAbsolutePath() + "/src/data/jflex/skeleton.default"
    }
    
    ant.taskdef(classname: 'jflex.anttask.JFlexTask',
    name: 'jflex',
    classpath: project.rootProject.ext.jflexPath)
    
    ant.jflex(file: inputDir.getAbsolutePath() + File.separator + fileName + ".jflex", outdir: target.getAbsolutePath(), nobak: 'on', skeleton: skeleton)
    
    if (disableBufferExpansion) {
      // Since the ZZ_BUFFERSIZE declaration is generated rather than in the skeleton, we have to transform it here.
      ant.replaceregexp(file: inputDir.getAbsolutePath() + File.separator + fileName +  ".java", match: "private static final int ZZ_BUFFERSIZE =", replace: "private int ZZ_BUFFERSIZE =")
    }
    
  }
  
}


