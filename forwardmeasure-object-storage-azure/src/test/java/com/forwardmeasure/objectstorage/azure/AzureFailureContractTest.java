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
package com.forwardmeasure.objectstorage.azure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientProvider;
import com.sun.net.httpserver.HttpServer;
import io.netty.channel.nio.NioEventLoopGroup;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AzureFailureContractTest {
  private static final String ACCOUNT = "contractaccount";
  private static final String KEY = java.util.Base64.getEncoder().encodeToString(new byte[32]);

  @Test
  void actualSdkHttpErrorsPreservePortableClassificationAcrossOperations() throws Exception {
    var status = new AtomicInteger(400);
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body =
              "<Error><Code>ControlledFailure</Code><Message>controlled provider failure</Message></Error>"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/xml");
          exchange.getResponseHeaders().set("x-ms-error-code", "ControlledFailure");
          if (exchange.getRequestMethod().equals("HEAD"))
            exchange.sendResponseHeaders(status.get(), -1);
          else {
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
    var loops = new NioEventLoopGroup(2);
    var sdk =
        new BlobServiceClientBuilder()
            .endpoint("http://127.0.0.1:" + server.getAddress().getPort() + "/" + ACCOUNT)
            .credential(new StorageSharedKeyCredential(ACCOUNT, KEY))
            .httpClient(new NettyAsyncHttpClientBuilder().eventLoopGroup(loops).build())
            .retryOptions(
                new RequestRetryOptions(
                    RetryPolicyType.FIXED,
                    1,
                    Duration.ofSeconds(5),
                    Duration.ofMillis(1),
                    Duration.ofMillis(1),
                    null))
            .buildClient();
    try (var storage = new AzureStorageClient(sdk, loops)) {
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
        assertEquals("azure", error.provider());
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
      loops.shutdownGracefully().syncUninterruptibly();
    }
  }

  @Test
  void discoveredProviderUsesInternalEndpointAndPublicCapabilitiesWithRequiredUploadHeaders() {
    var provider =
        ServiceLoader.load(StorageClientProvider.class).stream()
            .map(ServiceLoader.Provider::get)
            .filter(value -> value.backend().equals("azure"))
            .findFirst()
            .orElseThrow();
    for (String contentType : new String[] {null, " ", "text/plain"}) {
      try (var storage =
          provider.create(
              "evidence",
              config(
                  Optional.of("http://127.0.0.1:1/"),
                  Optional.of("https://public.example.test/" + ACCOUNT),
                  true,
                  true))) {
        assertEquals(
            "http://127.0.0.1:1/" + ACCOUNT,
            ((BlobServiceClient) storage.unwrap()).getAccountUrl());
        var get =
            storage.presignGet(
                new StorageClient.PresignGetRequest(
                    "bucket", "folder/a b.txt", Duration.ofMinutes(1)));
        assertEquals("public.example.test", get.uri().getHost());
        assertEquals("/bucket/folder/a b.txt", get.uri().getPath());
        assertTrue(get.uri().getQuery().contains("sp=r"));
        var put =
            storage.presignPut(
                new StorageClient.PresignPutRequest(
                    "bucket", "key", Duration.ofMinutes(1), contentType));
        assertEquals("public.example.test", put.uri().getHost());
        assertEquals("BlockBlob", put.requiredHeaders().get("x-ms-blob-type"));
        assertEquals(
            contentType != null && !contentType.isBlank(),
            put.requiredHeaders().containsKey("content-type"));
        assertTrue(put.uri().getQuery().contains("sp=cw"));
      }
    }
    try (var nativeClient =
        new AzureStorageClient(config(Optional.empty(), Optional.empty(), true, true))) {
      assertEquals(
          "https://" + ACCOUNT + ".blob.core.windows.net",
          ((BlobServiceClient) nativeClient.unwrap()).getAccountUrl());
    }
    for (boolean accountPresent : List.of(false, true)) {
      assertEquals(
          "INVALID_REQUEST",
          assertThrows(
                  StorageException.class,
                  () ->
                      new AzureStorageClient(
                          config(Optional.empty(), Optional.empty(), accountPresent, false)))
              .code());
    }
  }

  private static NamedStorageClientConfig config(
      Optional<String> endpoint, Optional<String> publicEndpoint, boolean account, boolean secret) {
    return (NamedStorageClientConfig)
        Proxy.newProxyInstance(
            NamedStorageClientConfig.class.getClassLoader(),
            new Class<?>[] {NamedStorageClientConfig.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "backend" -> "azure";
                  case "endpoint" -> endpoint;
                  case "publicEndpoint" -> publicEndpoint;
                  case "accessKey" -> account ? Optional.of(ACCOUNT) : Optional.empty();
                  case "secretKey" -> secret ? Optional.of(KEY) : Optional.empty();
                  case "properties" -> Map.of();
                  default -> Optional.empty();
                });
  }
}
