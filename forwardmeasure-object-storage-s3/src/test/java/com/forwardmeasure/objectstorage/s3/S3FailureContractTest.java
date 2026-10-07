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
package com.forwardmeasure.objectstorage.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Actual SDK HTTP responses verify the portable retry/error contract, without cloud credentials.
 */
class S3FailureContractTest {
  @Test
  void providerHttpFailuresPreserveErrorClassificationAcrossOperations() throws Exception {
    var status = new AtomicInteger(400);
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getRequestBody().readAllBytes();
          byte[] body =
              "<Error><Code>ControlledFailure</Code><Message>test failure</Message></Error>"
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().add("Content-Type", "application/xml");
          if (exchange.getRequestMethod().equals("HEAD"))
            exchange.sendResponseHeaders(status.get(), -1);
          else {
            exchange.sendResponseHeaders(status.get(), body.length);
            exchange.getResponseBody().write(body);
          }
          exchange.close();
        });
    server.start();
    String retryProperty = software.amazon.awssdk.core.SdkSystemSetting.AWS_MAX_ATTEMPTS.property();
    String original = System.getProperty(retryProperty);
    System.setProperty(retryProperty, "1");
    try (var storage =
        new S3StorageClient(config("http://127.0.0.1:" + server.getAddress().getPort()))) {
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
        assertEquals(expected.equals("RETRYABLE"), error.retryable());
        assertEquals("s3", error.provider());
      }
      status.set(403);
      List<Runnable> forbidden =
          List.of(
              () ->
                  storage.createBucket(new StorageClient.CreateBucketRequest("bucket", null, null)),
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
      for (var operation : forbidden)
        assertEquals("ACCESS_DENIED", assertThrows(StorageException.class, operation::run).code());
      assertEquals(
          "INTERNAL",
          assertThrows(
                  StorageException.class,
                  () ->
                      storage.presignGet(
                          new StorageClient.PresignGetRequest("bucket", "key", Duration.ofDays(8))))
              .code());
      assertEquals(
          "INTERNAL",
          assertThrows(
                  StorageException.class,
                  () ->
                      storage.presignPut(
                          new StorageClient.PresignPutRequest(
                              "bucket", "key", Duration.ofDays(8), null)))
              .code());
    } finally {
      if (original == null) System.clearProperty(retryProperty);
      else System.setProperty(retryProperty, original);
      server.stop(0);
    }
  }

  @Test
  void absentOrIncompleteStaticCredentialsKeepTheDefaultCredentialChain() {
    for (boolean includeAccessKey : List.of(false, true)) {
      var base = config("http://127.0.0.1:1");
      var noCredentials =
          (NamedStorageClientConfig)
              Proxy.newProxyInstance(
                  NamedStorageClientConfig.class.getClassLoader(),
                  new Class<?>[] {NamedStorageClientConfig.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("secretKey")
                        || (method.getName().equals("accessKey") && !includeAccessKey))
                      return Optional.empty();
                    return method.invoke(base, args);
                  });
      try (var storage = new S3StorageClient(noCredentials)) {
        var sdk = (software.amazon.awssdk.services.s3.S3Client) storage.unwrap();
        assertInstanceOf(
            software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.class,
            sdk.serviceClientConfiguration().credentialsProvider());
      }
    }
  }

  @Test
  void forcedBucketDeletionFollowsContinuationTokensBeforeDeletingTheBucket() throws Exception {
    var requests = new java.util.concurrent.CopyOnWriteArrayList<String>();
    var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          String request = exchange.getRequestMethod() + " " + exchange.getRequestURI();
          requests.add(request);
          String requestBody =
              new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          String response;
          if (exchange.getRequestMethod().equals("GET")) {
            boolean second =
                exchange.getRequestURI().toString().contains("continuation-token=page2");
            response =
                "<ListBucketResult"
                    + " xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>bucket</Name>"
                    + "<IsTruncated>"
                    + !second
                    + "</IsTruncated>"
                    + (second ? "" : "<NextContinuationToken>page2</NextContinuationToken>")
                    + "<Contents><Key>"
                    + (second ? "second" : "first")
                    + "</Key><Size>1</Size></Contents></ListBucketResult>";
          } else if (exchange.getRequestMethod().equals("POST")) {
            requests.add(requestBody);
            response = "<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>";
          } else {
            exchange.sendResponseHeaders(204, -1);
            exchange.close();
            return;
          }
          byte[] body = response.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/xml");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    server.start();
    try (var storage =
        new S3StorageClient(config("http://127.0.0.1:" + server.getAddress().getPort()))) {
      storage.deleteBucket(new StorageClient.DeleteBucketRequest("bucket", true));
      assertEquals(2, requests.stream().filter(value -> value.startsWith("GET ")).count());
      assertTrue(requests.stream().anyMatch(value -> value.contains("continuation-token=page2")));
      assertTrue(requests.stream().anyMatch(value -> value.contains("<Key>first</Key>")));
      assertTrue(requests.stream().anyMatch(value -> value.contains("<Key>second</Key>")));
      assertEquals("DELETE /bucket", requests.getLast());
    } finally {
      server.stop(0);
    }
  }

  private static NamedStorageClientConfig config(String endpoint) {
    return (NamedStorageClientConfig)
        Proxy.newProxyInstance(
            NamedStorageClientConfig.class.getClassLoader(),
            new Class<?>[] {NamedStorageClientConfig.class},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "backend" -> "s3";
                  case "endpoint" -> Optional.of(endpoint);
                  case "region" -> Optional.of("eu-west-1");
                  case "accessKey" -> Optional.of("test-key");
                  case "secretKey" -> Optional.of("test-secret");
                  case "pathStyleAccess" -> Optional.of(true);
                  case "properties" -> Map.of();
                  default -> Optional.empty();
                });
  }
}
