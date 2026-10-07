/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forwardmeasure.objectstorage.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageResourceLoaderTest {
  @TempDir Path directory;

  @Test
  void localFilesAndOpaqueOrHierarchicalClasspathUrisReadTheSameBytes() throws Exception {
    Path file = directory.resolve("source.txt");
    Files.writeString(file, "café");
    var loader = new StorageResourceLoader(null);
    assertEquals("café", loader.readString(file.toUri().toString()));
    var original = Thread.currentThread().getContextClassLoader();
    try (var resourceLoader =
        new java.net.URLClassLoader(new java.net.URL[] {directory.toUri().toURL()}, null)) {
      Thread.currentThread().setContextClassLoader(resourceLoader);
      assertEquals("café", loader.readString("classpath:source.txt"));
      assertEquals("café", loader.readString("classpath:/source.txt"));
      Thread.currentThread().setContextClassLoader(null);
      var fallback = new StorageResourceLoader(null, resourceLoader);
      assertEquals("café", fallback.readString("classpath:/source.txt", StandardCharsets.UTF_8));
      assertThrows(IOException.class, () -> fallback.readBytes("classpath:/missing.txt"));
      assertThrows(
          IOException.class,
          () -> new StorageResourceLoader(null, null).readBytes("classpath:/source.txt"));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }

  @Test
  void remoteObjectsAreClosedBeforeReturningAnIndependentStreamAndOnFailure() throws Exception {
    var provider = new StorageTestProvider();
    try (var registry =
        new StorageClientRegistry(StorageTestProvider.configuration(), List.of(provider))) {
      var loader = new StorageResourceLoader(registry);
      try (var input = loader.openStream("mem://bucket/folder/file%20name")) {
        assertEquals(1, provider.closedObjects.get());
        assertEquals("café", new String(input.readAllBytes(), StandardCharsets.UTF_8));
        assertEquals("bucket", provider.lastRequest.bucketName());
        assertEquals("folder/file name", provider.lastRequest.key());
      }
      var absent = assertThrows(IOException.class, () -> loader.readBytes("mem://bucket/missing"));
      assertTrue(absent.getCause() instanceof com.forwardmeasure.objectstorage.StorageException);
      provider.readFailure = true;
      var broken = assertThrows(IOException.class, () -> loader.readBytes("mem://bucket/key"));
      assertEquals("broken stream", broken.getMessage());
      assertEquals(2, provider.closedObjects.get());
    }
  }

  @Test
  void malformedAndUnconfiguredLocationsFailBeforeAnyProviderAccess() {
    var provider = new StorageTestProvider();
    try (var registry =
        new StorageClientRegistry(StorageTestProvider.configuration(), List.of(provider))) {
      var loader = new StorageResourceLoader(registry);
      for (String invalid :
          new String[] {
            null,
            " ",
            "no-scheme",
            "bad uri",
            "classpath:/",
            "mem:///key",
            "mem://bucket",
            "mem:opaque"
          }) assertThrows(IOException.class, () -> loader.readBytes(invalid));
      assertThrows(
          IllegalArgumentException.class, () -> loader.readString("mem://bucket/key", null));
      assertEquals(0, provider.created.get());
      assertThrows(
          IOException.class, () -> new StorageResourceLoader(null).readBytes("mem://bucket/key"));
    }
  }
}
