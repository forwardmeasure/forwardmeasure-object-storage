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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.forwardmeasure.objectstorage.core.StorageClientRegistry;
import com.forwardmeasure.objectstorage.core.StorageConfigGroup;
import com.forwardmeasure.testcontainers.minio.MinioTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Exercises the complete S3 adapter against a real MinIO server. */
class S3StorageClientIntegrationTest {

  @Test
  void supportsLifecycleStreamingRangesAndSignedCapabilities() throws Exception {
    try (var minio = new MinioTestContainer().start();
        var registry = new StorageClientRegistry(configurationGroup(minio))) {
      var storage = registry.getForUri(URI.create("s3://object-storage-contract/tenant/evidence"));
      assertSame(storage, registry.getDefault());
      assertSame(storage, registry.getForScheme("s3"));
      String bucket = "object-storage-contract";
      String key = "tenant/evidence/document.txt";
      byte[] content = "provider-neutral evidence".getBytes(StandardCharsets.UTF_8);

      storage.createBucket(
          new StorageClient.CreateBucketRequest(
              bucket, Map.of("purpose", "contract-test"), Map.of()));
      assertTrue(storage.bucketExists(bucket));

      var stored =
          storage.putObject(
              StorageClient.PutObjectRequest.fromBytes(
                  bucket, key, content, "text/plain", Map.of("tenant", "contract-test")));
      assertEquals(bucket, stored.bucketName());
      assertEquals(key, stored.key());

      var head = storage.headObject(new StorageClient.HeadObjectRequest(bucket, key));
      assertEquals(content.length, head.size());
      assertEquals("contract-test", head.userMetadata().get("tenant"));

      try (var object = storage.getObject(StorageClient.GetObjectRequest.of(bucket, key))) {
        assertEquals("text/plain", object.metadata().contentType().orElseThrow());
        assertArrayEquals(content, object.content().readAllBytes());
      }

      try (var range =
          storage.getObject(new StorageClient.GetObjectRequest(bucket, key, 9L, 15L))) {
        assertEquals("neutral", new String(range.content().readAllBytes(), StandardCharsets.UTF_8));
      }

      String uploadKey = "tenant/evidence/signed.txt";
      var putCapability =
          storage.presignPut(
              new StorageClient.PresignPutRequest(
                  bucket, uploadKey, Duration.ofMinutes(5), "text/plain"));
      var http = HttpClient.newHttpClient();
      var put =
          http.send(
              request(putCapability)
                  .PUT(HttpRequest.BodyPublishers.ofString("signed upload"))
                  .build(),
              HttpResponse.BodyHandlers.discarding());
      assertTrue(put.statusCode() >= 200 && put.statusCode() < 300);

      var getCapability =
          storage.presignGet(
              new StorageClient.PresignGetRequest(bucket, uploadKey, Duration.ofMinutes(5)));
      var get =
          http.send(request(getCapability).GET().build(), HttpResponse.BodyHandlers.ofString());
      assertEquals(200, get.statusCode());
      assertEquals("signed upload", get.body());

      assertEquals(
          2, storage.listObjects(StorageClient.ListObjectsRequest.of(bucket)).objects().size());

      storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, key));
      storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, uploadKey));
      assertThrows(
          StorageException.class,
          () -> storage.headObject(new StorageClient.HeadObjectRequest(bucket, key)));

      storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
      assertFalse(storage.bucketExists(bucket));
    }
  }

  /**
   * Objects carry their full-object CRC32C, and a read names one version: on a versioned bucket an
   * overwritten version still reads back exactly its own bytes.
   */
  @Test
  void reportsChecksumsAndReadsOnlyThePinnedVersion() throws Exception {
    try (var minio = new MinioTestContainer().start();
        var registry = new StorageClientRegistry(configurationGroup(minio))) {
      var storage = registry.getDefault();
      String bucket = "object-storage-pinning";
      String key = "sources/feed.csv";
      byte[] first = "uid,name\n1,First\n".getBytes(StandardCharsets.UTF_8);
      byte[] second = "uid,name\n2,Second\n".getBytes(StandardCharsets.UTF_8);
      storage.createBucket(new StorageClient.CreateBucketRequest(bucket, Map.of(), Map.of()));
      // Versioning is a bucket setting outside this client's API; set it on the native client.
      ((software.amazon.awssdk.services.s3.S3Client) storage.unwrap())
          .putBucketVersioning(
              b ->
                  b.bucket(bucket)
                      .versioningConfiguration(
                          v ->
                              v.status(
                                  software.amazon.awssdk.services.s3.model.BucketVersioningStatus
                                      .ENABLED)));

      String firstVersion =
          storage
              .putObject(
                  StorageClient.PutObjectRequest.fromBytes(
                      bucket, key, first, "text/csv", Map.of()))
              .versionId();
      var original =
          storage.headObject(new StorageClient.HeadObjectRequest(bucket, key, firstVersion));
      assertEquals(firstVersion, original.versionId());
      assertEquals(crc32c(first), original.checksums().crc32c());

      storage.putObject(
          StorageClient.PutObjectRequest.fromBytes(bucket, key, second, "text/csv", Map.of()));
      var live = storage.headObject(new StorageClient.HeadObjectRequest(bucket, key));
      assertFalse(firstVersion.equals(live.versionId()), "an overwrite is a new version");
      assertEquals(crc32c(second), live.checksums().crc32c());

      try (var pinned =
          storage.getObject(StorageClient.GetObjectRequest.ofVersion(bucket, key, firstVersion))) {
        assertEquals(firstVersion, pinned.metadata().versionId().orElseThrow());
        assertEquals(crc32c(first), pinned.metadata().checksums().crc32c());
        assertArrayEquals(first, pinned.content().readAllBytes());
      }
    }
  }

  private static String crc32c(byte[] content) {
    var crc = new java.util.zip.CRC32C();
    crc.update(content);
    return java.util.Base64.getEncoder()
        .encodeToString(java.nio.ByteBuffer.allocate(4).putInt((int) crc.getValue()).array());
  }

  @Test
  void separatesInternalOperationsFromPublicSigningAndSupportsFilesStreamsAndPagination()
      throws Exception {
    try (var minio = new MinioTestContainer().start()) {
      var base = configuration(minio);
      var config =
          (NamedStorageClientConfig)
              java.lang.reflect.Proxy.newProxyInstance(
                  NamedStorageClientConfig.class.getClassLoader(),
                  new Class<?>[] {NamedStorageClientConfig.class},
                  (proxy, method, args) ->
                      method.getName().equals("publicEndpoint")
                          ? Optional.of("https://public-storage.example.test")
                          : method.invoke(base, args));
      try (var storage = new S3StorageClient(config)) {
        String bucket = "extended-contract";
        storage.createBucket(new StorageClient.CreateBucketRequest(bucket, null, null));
        assertTrue(storage.getBucket(bucket).isPresent());
        assertTrue(storage.listBuckets().stream().anyMatch(value -> value.name().equals(bucket)));
        assertTrue(storage.unwrap() instanceof software.amazon.awssdk.services.s3.S3Client);
        var file = java.nio.file.Files.createTempFile("s3-contract", ".txt");
        try {
          java.nio.file.Files.writeString(file, "abcdef");
          storage.putObject(
              StorageClient.PutObjectRequest.fromPath(bucket, "a.txt", file, null, null));
          try (var stream =
              new java.io.ByteArrayInputStream("stream".getBytes(StandardCharsets.UTF_8))) {
            storage.putObject(
                StorageClient.PutObjectRequest.fromStream(bucket, "b.txt", stream, 6, null, null));
          }
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
        var first =
            storage.listObjects(new StorageClient.ListObjectsRequest(bucket, null, null, 1, " "));
        assertTrue(first.isTruncated());
        var next =
            storage.listObjects(
                new StorageClient.ListObjectsRequest(
                    bucket, null, null, 1, first.nextContinuationToken()));
        assertEquals(1, next.objects().size());
        assertFalse(first.objects().getFirst().key().equals(next.objects().getFirst().key()));
        try (var tail =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", 3L, null));
            var head =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", null, 2L))) {
          assertEquals(bucket, tail.bucketName());
          assertEquals("a.txt", tail.key());
          assertEquals("def", new String(tail.content().readAllBytes(), StandardCharsets.UTF_8));
          assertEquals("abc", new String(head.content().readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals(
            "public-storage.example.test",
            storage
                .presignGet(
                    new StorageClient.PresignGetRequest(bucket, "a.txt", Duration.ofMinutes(1)))
                .uri()
                .getHost());
        for (String contentType : new String[] {null, " "})
          assertEquals(
              "public-storage.example.test",
              storage
                  .presignPut(
                      new StorageClient.PresignPutRequest(
                          bucket, "signed", Duration.ofMinutes(1), contentType))
                  .uri()
                  .getHost());
        var conflict =
            assertThrows(
                StorageException.class,
                () -> storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, false)));
        assertEquals("CONFLICT", conflict.code());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
        assertTrue(storage.getBucket(bucket).isEmpty());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, false));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, "missing"));
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () -> storage.getObject(StorageClient.GetObjectRequest.of(bucket, "missing")))
                .code());
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.putObject(
                            StorageClient.PutObjectRequest.fromBytes(
                                bucket, "key", new byte[0], null, null)))
                .code());
      }
    }
  }

  private static NamedStorageClientConfig configuration(MinioTestContainer minio) {
    return new NamedStorageClientConfig() {
      @Override
      public String backend() {
        return "s3";
      }

      @Override
      public Optional<String> bucket() {
        return Optional.empty();
      }

      @Override
      public Optional<String> endpoint() {
        return Optional.of(minio.hostEndpoint().toString());
      }

      @Override
      public Optional<String> publicEndpoint() {
        return Optional.of(minio.hostEndpoint().toString());
      }

      @Override
      public Optional<String> region() {
        return Optional.of("us-east-1");
      }

      @Override
      public Optional<String> accessKey() {
        return Optional.of(minio.accessKey());
      }

      @Override
      public Optional<String> secretKey() {
        return Optional.of(minio.secretKey());
      }

      @Override
      public Optional<Boolean> pathStyleAccess() {
        return Optional.of(true);
      }

      @Override
      public Map<String, String> properties() {
        return Map.of();
      }
    };
  }

  private static StorageConfigGroup configurationGroup(MinioTestContainer minio) {
    return new StorageConfigGroup() {
      @Override
      public Optional<String> defaultClient() {
        return Optional.of("evidence");
      }

      @Override
      public Map<String, NamedStorageClientConfig> clients() {
        return Map.of("evidence", configuration(minio));
      }

      @Override
      public Map<String, String> schemeClients() {
        return Map.of("s3", "evidence");
      }

      @Override
      public Map<String, String> uriClients() {
        return Map.of();
      }
    };
  }

  private static HttpRequest.Builder request(StorageClient.PresignedRequest capability) {
    var builder = HttpRequest.newBuilder(capability.uri());
    capability.requiredHeaders().forEach(builder::header);
    return builder;
  }
}
