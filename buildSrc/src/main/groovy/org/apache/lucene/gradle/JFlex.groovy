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

/** Task script that is called by Ant's build.xml file:
 * Checks GIT working copy for unversioned or modified files.
 */

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

class JFlex extends DefaultTask {
  
  @InputFile
  File source
  
  @OutputDirectory
  File target
  
  @TaskAction
  void jflex() {
    
    ant.taskdef(classname: 'jflex.anttask.JFlexTask',
    name: 'jflex',
    classpath: project.configurations.jflex.asPath)
    
    
    ant.jflex(file: source.getAbsolutePath(), outdir: target.getAbsolutePath(), nobak: 'on', skeleton: "src/data/jflex/skeleton.default")
    
  }
}


