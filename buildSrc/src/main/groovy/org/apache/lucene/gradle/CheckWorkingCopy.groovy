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

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.errors.*;
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction

import javax.inject.Inject

class CheckWorkingCopy extends DefaultTask {

  @Input
  boolean failonmodifications

  @Inject
  CheckWorkingCopy(boolean failonmodifications) {
    this.failonmodifications = failonmodifications
    this.group = 'Verification'
    this.description = "Checks the local project working copy."
  }

  @TaskAction
  void check() {
    try {
      println 'Initializing working copy...'
      final Repository repository = new FileRepositoryBuilder()
        .setWorkTree(project.getRootDir())
        .setMustExist(true)
        .build();

      println 'Checking working copy status...'
      final Status status = new Git(repository).status().call();
      if (!status.isClean()) {
        final SortedSet unversioned = new TreeSet(), modified = new TreeSet();
        status.properties.each{ prop, val ->
          if (val instanceof Set) {
            if (prop in ['untracked', 'untrackedFolders', 'missing']) {
              unversioned.addAll(val);
            } else if (prop != 'ignoredNotInIndex') {
              modified.addAll(val);
            }
          }
        }
        if (unversioned) {
          throw new GradleException(
                  "Source checkout is dirty (unversioned/missing files) after running tests!!! Offending files:\n" +
                  '* ' + unversioned.join('\n* '))
        }
        if (failonmodifications && modified) {
          throw new GradleException(
                  "Source checkout is modified!!! Offending files:\n"+
                          '* ' + modified.join('\n* '))
        }
      }
    } catch (RepositoryNotFoundException | NoWorkTreeException | NotSupportedException e) {
      logger.error('WARNING: Development directory is not a valid GIT checkout! Disabling checks...')
    }
  }
}


