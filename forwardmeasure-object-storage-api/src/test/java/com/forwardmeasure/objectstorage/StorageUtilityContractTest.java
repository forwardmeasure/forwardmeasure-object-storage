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
package com.forwardmeasure.objectstorage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class StorageUtilityContractTest {
  @Test
  void uploadBufferIsCompleteAndDeletedOnBothSuccessAndProviderFailure() throws Exception {
    for (boolean fail : List.of(false, true)) {
      var path = new AtomicReference<Path>();
      var closed = new java.util.concurrent.atomic.AtomicBoolean();
      var input =
          new ByteArrayInputStream(new byte[] {1, 2, 3}) {
            @Override
            public void close() throws IOException {
              closed.set(true);
              super.close();
            }
          };
      var providerFailure = StorageException.retryable("s3", "upload unavailable", null);
      var action =
          (java.util.function.BiFunction<Path, Long, String>)
              (file, size) -> {
                path.set(file);
                assertEquals(3L, size);
                assertTrue(closed.get(), "Input must be closed before provider upload starts");
                try {
                  assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(file));
                } catch (IOException error) {
                  throw new AssertionError(error);
                }
                if (fail) throw providerFailure;
                return "uploaded";
              };
      if (fail)
        assertSame(
            providerFailure,
            assertThrows(
                StorageException.class,
                () -> StorageClient.bufferAndUpload(input, "storage-contract", ".bin", action)));
      else
        assertEquals(
            "uploaded", StorageClient.bufferAndUpload(input, "storage-contract", ".bin", action));
      assertFalse(Files.exists(path.get()));
    }
  }

  @Test
  void bestEffortDeleteTargetsOnlyValidObjectsAndDoesNotMaskOtherCleanup() {
    var deleted = new ArrayList<StorageClient.DeleteObjectRequest>();
    var failure = new AtomicReference<RuntimeException>();
    var client =
        (StorageClient)
            Proxy.newProxyInstance(
                StorageClient.class.getClassLoader(),
                new Class<?>[] {StorageClient.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("deleteObject"))
                    throw new AssertionError(method.getName());
                  deleted.add((StorageClient.DeleteObjectRequest) args[0]);
                  if (failure.get() != null) throw failure.get();
                  return null;
                });
    for (String invalid :
        new String[] {null, " ", "not a URI", "relative", "urn:opaque", "s3:///key", "s3://bucket"})
      StorageClient.deleteBestEffort(client, invalid, "contract");
    assertTrue(deleted.isEmpty());
    StorageClient.deleteBestEffort(client, "s3://bucket/folder/file%20name", "contract");
    assertEquals(
        List.of(new StorageClient.DeleteObjectRequest("bucket", "folder/file name")), deleted);
    failure.set(StorageException.accessDenied("s3", "denied", null));
    StorageClient.deleteBestEffort(client, "s3://bucket/key", "contract");
    failure.set(new IllegalStateException("transport failed"));
    StorageClient.deleteBestEffort(client, "s3://bucket/key", "contract");
    assertEquals(3, deleted.size());
  }

  @Test
  void backendAliasesProduceCanonicalStorageUris() {
    for (String backend : List.of("s3", "s3-compat", " S3 "))
      assertEquals("s3://bucket/key", StorageClient.buildPlatformUri(backend, "bucket", "key"));
    assertEquals("gs://bucket/key", StorageClient.buildPlatformUri("gcs", "bucket", "key"));
    for (String backend : List.of("azure", "azure-blob"))
      assertEquals("az://bucket/key", StorageClient.buildPlatformUri(backend, "bucket", "key"));
    for (String backend : new String[] {null, "unknown"})
      assertThrows(
          IllegalArgumentException.class,
          () -> StorageClient.buildPlatformUri(backend, "bucket", "key"));
  }

  @Test
  void errorClassificationPreservesCauseAndOnlyTransientFailuresRecommendRetry() {
    var cause = new IOException("provider failure");
    var errors =
        List.of(
            StorageException.notFound("s3", "missing", cause),
            StorageException.accessDenied("s3", "denied", cause),
            StorageException.conflict("s3", "conflict", cause),
            StorageException.invalidRequest("s3", "invalid", cause),
            StorageException.internal("s3", "internal", cause));
    assertEquals(
        List.of("NOT_FOUND", "ACCESS_DENIED", "CONFLICT", "INVALID_REQUEST", "INTERNAL"),
        errors.stream().map(StorageException::code).toList());
    for (var error : errors) {
      assertFalse(error.retryable());
      assertSame(cause, error.getCause());
      assertEquals("s3", error.provider());
    }
    assertTrue(StorageException.retryable("s3", "retry", cause).retryable());
    assertFalse(new StorageException("failure").retryable());
    assertSame(cause, new StorageException("failure", cause).getCause());
    assertEquals("CUSTOM", new StorageException("failure", "s3", "CUSTOM", false).code());
  }
}
