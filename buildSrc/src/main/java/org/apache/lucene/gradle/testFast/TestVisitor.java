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
package org.apache.lucene.gradle.testFast;


import org.objectweb.asm.AnnotationVisitor;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

/**
 * Base class for ASM test class scanners.
 */
public class TestVisitor extends ClassVisitor {
  
  private boolean hasSlowAnnotation = false;
  private boolean hasSlowestAnnotation = false;
  private boolean hasNightlyAnnotation = false;
  private boolean hasIgnoreAnnotation = false;
  
  TestVisitor() {
    super(Opcodes.ASM7);
  }
  
  public void visitOuterClass(final String owner, final String name, final String descriptor) {
    super.visitOuterClass(owner, name, descriptor);
  }

  @Override
  public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(version, access, name, signature, superName, interfaces);
  }
  
  @Override
  public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
    
    if ("Lorg/apache/lucene/util/LuceneTestCase$Slowest;".equals(desc)) {
      hasSlowestAnnotation = true;
    }
    
    if ("Lorg/apache/lucene/util/LuceneTestCase$Slow;".equals(desc)) {
      hasSlowAnnotation = true;
    }

    if ("Lorg/apache/lucene/util/LuceneTestCase$Nightly;".equals(desc) || "Lcom/carrotsearch/randomizedtesting/annotations/Nightly;".equals(desc)) {
      hasNightlyAnnotation = true;
    }
    
    if ("Lorg/junit/Ignore;".equals(desc)) {
      hasIgnoreAnnotation = true;
    }
    
    return super.visitAnnotation(desc, visible);
  }
  
  public boolean hasSlowAnnotation() {
    return hasSlowAnnotation;
  }
  
  public boolean hasSlowestAnnotation() {
    return hasSlowestAnnotation;
  }
  
  public boolean hasNightlyAnnotation() {
    return hasNightlyAnnotation;
  }
  
  public boolean hasIgnoreAnnotation() {
    return hasIgnoreAnnotation;
  }

}
