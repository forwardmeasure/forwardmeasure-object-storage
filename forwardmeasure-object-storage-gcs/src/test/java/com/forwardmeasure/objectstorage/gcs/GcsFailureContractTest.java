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
package com.forwardmeasure.objectstorage.gcs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Exercises error classification and deletion ordering through the actual Google HTTP SDK. */
class GcsFailureContractTest {
  @Test
  void httpFailuresPreservePortableClassificationAndIdempotentDeletes() throws Exception {
    var status = new AtomicInteger(400);
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body =
              ("{\"error\":{\"code\":"
                      + status.get()
                      + ",\"message\":\"controlled provider failure\"}}")
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(status.get(), body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try (var storage = client(server)) {
      for (int code : new int[] {400, 403, 404, 409, 429, 500, 502, 503, 504}) {
        status.set(code);
        var error = assertThrows(StorageException.class, storage::listBuckets);
        String expected =
            switch (code) {
              case 403 -> "ACCESS_DENIED";
              case 404 -> "NOT_FOUND";
              case 409 -> "CONFLICT";
              case 429, 500, 502, 503, 504 -> "RETRYABLE";
              default -> "INTERNAL";
            };
        assertEquals(expected, error.code());
        assertEquals("gcs", error.provider());
        assertEquals(expected.equals("RETRYABLE"), error.retryable());
      }
      status.set(403);
      List<Runnable> forbidden =
          List.of(
              () ->
                  storage.createBucket(new StorageClient.CreateBucketRequest("bucket", null, null)),
              () -> storage.deleteBucket(new StorageClient.DeleteBucketRequest("bucket", false)),
              () -> storage.bucketExists("bucket"),
              () -> storage.getBucket("bucket"),
              () -> storage.listObjects(StorageClient.ListObjectsRequest.of("bucket")),
              () -> storage.headObject(new StorageClient.HeadObjectRequest("bucket", "key")),
              () -> storage.getObject(StorageClient.GetObjectRequest.of("bucket", "key")),
              () ->
                  storage.putObject(
                      StorageClient.PutObjectRequest.fromBytes(
                          "bucket", "key", new byte[0], null, null)),
              () -> storage.deleteObject(new StorageClient.DeleteObjectRequest("bucket", "key")));
      for (var operation : forbidden) {
        assertEquals("ACCESS_DENIED", assertThrows(StorageException.class, operation::run).code());
      }
      status.set(404);
      assertFalse(storage.bucketExists("missing"));
      assertTrue(storage.getBucket("missing").isEmpty());
      storage.deleteBucket(new StorageClient.DeleteBucketRequest("missing", false));
      storage.deleteObject(new StorageClient.DeleteObjectRequest("missing", "key"));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void sparseMetadataAndPagedForcedDeletionRetainEveryObjectAndDeletionOrder() throws Exception {
    var requests = new java.util.concurrent.CopyOnWriteArrayList<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          String request = exchange.getRequestMethod() + " " + exchange.getRequestURI();
          requests.add(request);
          exchange.getRequestBody().readAllBytes();
          if (exchange.getRequestMethod().equals("DELETE")) {
            exchange.sendResponseHeaders(204, -1);
          } else {
            String payload;
            if (exchange.getRequestURI().getPath().endsWith("/o/first")) {
              payload = "{\"bucket\":\"bucket\",\"name\":\"first\"}";
            } else {
              boolean second = request.contains("pageToken=next");
              payload =
                  "{\"items\":[{\"bucket\":\"bucket\",\"name\":\""
                      + (second ? "second" : "first")
                      + "\"}]"
                      + (second ? "}" : ",\"nextPageToken\":\"next\"}");
            }
            byte[] body = payload.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
    try (var storage = client(server)) {
      var listed = storage.listObjects(StorageClient.ListObjectsRequest.of("bucket"));
      assertEquals(1, listed.objects().size());
      assertEquals(0, listed.objects().getFirst().size());
      assertTrue(listed.objects().getFirst().userMetadata().isEmpty());
      assertTrue(listed.isTruncated());
      var head = storage.headObject(new StorageClient.HeadObjectRequest("bucket", "first"));
      assertEquals(0, head.size());
      assertTrue(head.userMetadata().isEmpty());
      try (var object = storage.getObject(StorageClient.GetObjectRequest.of("bucket", "first"))) {
        assertEquals(0, object.metadata().contentLength());
        assertTrue(object.metadata().userMetadata().isEmpty());
      }
      var closed = new AtomicInteger();
      var seekFailure = new java.io.IOException("controlled seek failure");
      var closeFailure = new IllegalStateException("controlled close failure");
      var channel =
          (com.google.cloud.ReadChannel)
              Proxy.newProxyInstance(
                  com.google.cloud.ReadChannel.class.getClassLoader(),
                  new Class<?>[] {com.google.cloud.ReadChannel.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("seek")) throw seekFailure;
                    if (method.getName().equals("close")) {
                      closed.incrementAndGet();
                      throw closeFailure;
                    }
                    throw new AssertionError("Unexpected channel operation " + method.getName());
                  });
      var sdk = (com.google.cloud.storage.Storage) storage.unwrap();
      var failingReader =
          (com.google.cloud.storage.Storage)
              Proxy.newProxyInstance(
                  com.google.cloud.storage.Storage.class.getClassLoader(),
                  new Class<?>[] {com.google.cloud.storage.Storage.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("reader")) return channel;
                    try {
                      return method.invoke(sdk, args);
                    } catch (java.lang.reflect.InvocationTargetException error) {
                      throw error.getCause();
                    }
                  });
      try (var failureClient =
          new GcsStorageClient(
              config(sdk.getOptions().getHost()), failingReader, NoCredentials.getInstance())) {
        var error =
            assertThrows(
                StorageException.class,
                () ->
                    failureClient.getObject(
                        new StorageClient.GetObjectRequest("bucket", "first", 1L, null)));
        assertEquals("INTERNAL", error.code());
        assertEquals(seekFailure, error.getCause());
        assertEquals(List.of(closeFailure), List.of(error.getCause().getSuppressed()));
        assertEquals(1, closed.get());
      }
      requests.clear();
      storage.deleteBucket(new StorageClient.DeleteBucketRequest("bucket", true));
      assertEquals(5, requests.size());
      assertTrue(requests.get(0).startsWith("GET /storage/v1/b/bucket/o"));
      assertEquals("DELETE /storage/v1/b/bucket/o/first", requests.get(1));
      assertTrue(requests.get(2).contains("pageToken=next"));
      assertEquals("DELETE /storage/v1/b/bucket/o/second", requests.get(3));
      assertEquals("DELETE /storage/v1/b/bucket", requests.get(4));
    } finally {
      server.stop(0);
    }
  }

  @Test
  void serviceDiscoveryCreatesAnonymousEmulatorClientWithoutCloudCredentialLookup() {
    var provider =
        ServiceLoader.load(StorageClientProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(value -> value.backend().equals("gcs"))
            .findFirst()
            .orElseThrow();
    try (var storage = provider.create("emulator", config("http://127.0.0.1:1"))) {
      var sdk = (com.google.cloud.storage.Storage) storage.unwrap();
      assertEquals("http://127.0.0.1:1", sdk.getOptions().getHost());
      assertTrue(sdk.getOptions().getCredentials() instanceof NoCredentials);
    }
  }

  private static GcsStorageClient client(HttpServer server) {
    String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
    var sdk =
        StorageOptions.newBuilder()
            .setHost(endpoint)
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .setRetrySettings(
                StorageOptions.getDefaultRetrySettings().toBuilder().setMaxAttempts(1).build())
            .build()
            .getService();
    return new GcsStorageClient(config(endpoint), sdk, NoCredentials.getInstance());
  }

  private static NamedStorageClientConfig config(String endpoint) {
    return (NamedStorageClientConfig)
        Proxy.newProxyInstance(
            NamedStorageClientConfig.class.getClassLoader(),
            new Class<?>[] {NamedStorageClientConfig.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "backend" -> "gcs";
                  case "endpoint" -> Optional.of(endpoint);
                  case "properties" -> Map.of("gcp.projectId", "test-project");
                  default -> Optional.empty();
                });
  }
}
