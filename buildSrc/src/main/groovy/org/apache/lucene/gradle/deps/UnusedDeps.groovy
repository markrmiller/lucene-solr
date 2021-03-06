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

import org.gradle.api.artifacts.ResolvedArtifact
import org.gradle.api.artifacts.ResolvedDependency
import javax.inject.Inject
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.file.RelativePath
import org.gradle.api.internal.artifacts.dependencies.DefaultProjectDependency
import org.gradle.api.specs.Spec
import org.gradle.api.specs.Specs
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.TaskAction

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Matcher
import java.util.regex.Pattern
import java.util.zip.ZipFile
import java.util.zip.ZipException

import java.util.stream.Stream

// dev util task to help find possible unused deps
class UnusedDeps extends DefaultTask {
  protected static Pattern pattern = Pattern.compile("\\(([^\\s]*?\\.jar)\\)")
  
  protected configuration = "runtimeClasspath"
  
  File inputDirectory

  private List<String> jarExcludes = new ArrayList<>()

  public UnusedDeps() {
    
  }
  
  @InputDirectory
  public File getInputDirectory() {
    return inputDirectory;
  }
  
  @Input
  @Optional
  public List<String> getJarExcludes() {
    return jarExcludes;
  }
  
  @TaskAction
  void execute() {
    if (project.hasProperty('useConfiguration')) {
      configuration = project.useConfiguration
    }
    
    if (!project.configurations.hasProperty(configuration)) {
      return
    }
    
    // make sure ant task logging shows up by default
    ant.lifecycleLogLevel = "INFO"
    
    Configuration config = project.configurations[this.configuration]
    
    List<Project> buildProjects = new ArrayList()
    buildProjects.add(project)
    config.getAllDependencies().forEach({ dep ->
      if (dep instanceof DefaultProjectDependency) {
        Project dProject = dep.getDependencyProject()
        buildProjects.add(dProject)
      }
    })
    
    File distDir = new File(inputDirectory, 'distDir')
    File jdepsDir = new File(inputDirectory, 'jdepsDir')
    
    def topLvlProject = getTopLvlProject(project)
    
    Set<String> usedStaticallyJarNames = getStaticallyReferencedDeps(topLvlProject, project, distDir, jdepsDir)
    
    Set<File> ourDeps = getAllDefinedDeps(project, config)
    
    Set<File> ourImmediatelyDefinedDeps = getOurImmediateDefinedDeps(project, config)
    
    config.getAllDependencies().forEach({ dep ->
      if (dep instanceof DefaultProjectDependency) {
        // println 'process project ' + dep.getDependencyProject()
        Project dProject = dep.getDependencyProject()
        def depTopLvlDProject = getTopLvlProject(dProject)
        
        Set<String> projectUsedDeps = getStaticallyReferencedDeps(depTopLvlDProject, dProject, distDir, jdepsDir)
        // println 'project deps: ' + projectUsedDeps
        usedStaticallyJarNames += projectUsedDeps
      }
    })
    
    usedStaticallyJarNames -= [
      "${project.name}-${project.version}.jar"
    ]
    
    Set<String> ourDepJarNames = new HashSet<>()
    ourDeps.forEach( { ourDepJarNames.add(it.getName()) } )
    
    Set<String> unusedJarNames = new HashSet<>()
    unusedJarNames.addAll(ourDepJarNames)
    unusedJarNames -= usedStaticallyJarNames
    unusedJarNames = unusedJarNames.toSorted()
    
    Set<String> depsInDirectUse = new HashSet<>()
    
    File jdepsLucene = new File(jdepsDir, "lucene")
    File jdepsSolr = new File(jdepsDir, "solr")
    
    for (File file : jdepsLucene.listFiles()) {
      lookForDep(file, depsInDirectUse)
    }
    for (File file : jdepsSolr.listFiles()) {
      lookForDep(file, depsInDirectUse)
    }
    
    println ''
    println 'Our classpath dependency count ' + ourDepJarNames.size()
    println 'Our directly declared dependency count ' + usedStaticallyJarNames.size()
    println ''
    println 'List of possibly unused jars - they may be used at runtime however (Class.forName on plugins or other dynamic Object instantiation for example). This is not definitive, but helps narrow down what to investigate.'
    println 'We take our classpath dependencies, substract our direct dependencies and then subtract dependencies used by our direct dependencies.'
    println ''
    
    println 'Direct deps that may be unused:'
    
    boolean failTask = false
    
    unusedJarNames.forEach({
      if (!depsInDirectUse.contains(it) && ourImmediatelyDefinedDeps.contains(it)) {
        
        for (String exclude : jarExcludes) {
          if (it.matches(exclude)) {
            println ' - (excluded) violation : ' + it
            return
          }
        }
        
        if (findInSrc(it)) {
          println ' - violation : ' + it + ' *'
        } else {
          failTask = true
          println ' - violation : ' + it
        }
        
      }
    })
    
    println ''
    println 'Deps brought in by other modules that may be unused in this module (this is expected to happen and mostly informational):'
    unusedJarNames.forEach({
      if (!depsInDirectUse.contains(it) && !ourImmediatelyDefinedDeps.contains(it)) {
        println ' - ' + it
      }
    })
    
    if (failTask) {
      throw new GradleException("Unused dependencies found! Remove them or add an exclusion if they are actually necessary.")
    }
  }
  
  static class NonProjectSpec implements Spec<Dependency> {
    @Override
    public boolean isSatisfiedBy(Dependency dep) {
      return true
    }
  }
  
  protected Set getAllDefinedDeps(Project project, Configuration config) {
    Set<File> ourDeps = new HashSet<>()
    
    if (config.isCanBeResolved()) {
      config.getResolvedConfiguration().getResolvedArtifacts().forEach( { ra -> if (ra.getFile().exists()) ourDeps.add(ra.getFile()) })
    }
    
    return ourDeps
  }
  
  protected Set getOurImmediateDefinedDeps(Project project, Configuration config) {
    Set<String> ourDeps = new HashSet<>()
    
    Set<ResolvedDependency> deps = project.configurations.runtimeClasspath.getResolvedConfiguration().getFirstLevelModuleDependencies(new NonProjectSpec())
    
    for (ResolvedDependency dep : deps) {
      dep.getModuleArtifacts().forEach({ourDeps.add(it.file.name)})
    }
    
    return ourDeps
  }
  
  protected Set getStaticallyReferencedDeps(Project topLvlProject, Project project, File distDir, File jdepsDir) {
    
    File dotFile = project.mfile(jdepsDir, topLvlProject.name +  "/" + "${project.name}-${project.version}/${project.name}-${project.version}.jar.dot")
    Set<String> usedStaticallyJarNames = getDirectlyUsedJars(project, dotFile)
    
    return usedStaticallyJarNames
  }
  
  protected Set getDirectlyUsedJars(Project project, File dotFile) {
    Set<String> usedStaticallyJarNames = new HashSet<>()
    
    if (!dotFile.exists()) {
      println 'Could not find dotfile!! ' + dotFile
      return Collections.emptySet()
    }
    
    def lines = dotFile.readLines()
    String lastName = ""
    for (String line : lines) {
      Matcher m = pattern.matcher(line)
      if (m.find()) {
        String jarName = m.group(1)
        if (!lastName.equals(jarName)) {
          usedStaticallyJarNames.add(jarName)
        }
        lastName = jarName
      }
    }
    return usedStaticallyJarNames
  }
  
  protected void lookForDep(File dir, Set<String> depsInDirectUse) {
    dir.eachFile() {
      depsInDirectUse.addAll(getDirectlyUsedJars(project, it))
    }
  }
  
  protected boolean findInSrc(String jarName) {
    println ''
    println '   searching for occurrences in source ... (will only list some of them if found): ' + jarName
    println ''
    
    AtomicBoolean foundInsrc = new AtomicBoolean(false)
    AtomicInteger cnt = new AtomicInteger()
    
    def files = project.configurations[configuration].resolvedConfiguration.getFiles()

    FindInSrc findInSrc = new FindInSrc(project)
    Stream.of(files.toArray())
        .parallel()
        .forEach( { file ->
          if (!file.exists() || !file.name.equals(jarName)) return
            try {
              
              // bail if we find one match for now
              if (foundInsrc.get()) {
                return
              }
              ZipFile zip = new ZipFile(file)
              
              zip.stream().parallel()
              .forEach( { entry ->
                
                // bail if we find one match for now
                if (foundInsrc.get()) {
                  return
                }
                
                if (!entry.isDirectory() && entry.getName().endsWith(".class") && !entry.getName().equals('module-info.class')) {
                  String className = entry.getName().replace('/', '.')
                  className = className.substring(0, className.length() - ".class".length())
                  //println 'class: ' + className
                  
                  def found = (findInSrc.find(jarName.substring(0, jarName.length() - ".jar".length()), className))
                  if (found) {
                    foundInsrc.set(true)
                    cnt.incrementAndGet()
                  }
                }
              })
            } catch (ZipException zipEx) {
              println "Unable to open file ${file.name}"
            }
        })
    
    return foundInsrc.get()
  }
  
  public UnusedDeps jarExclude(String... arg0) {
    for (String pattern : arg0) {
      jarExcludes.add(pattern)
    }
    return this
  }
  
  protected Project getTopLvlProject(Project proj) {
    def topLvlProject
    if (proj.group ==~ /.*?\.lucene(?:\.\w+)?/) {
      topLvlProject = project.project(":lucene")
    } else if (proj.group ==~ /.*?\.solr(?:\.\w+)?/) {
      topLvlProject = project.project(":solr")
    }
    return topLvlProject
  }
}


