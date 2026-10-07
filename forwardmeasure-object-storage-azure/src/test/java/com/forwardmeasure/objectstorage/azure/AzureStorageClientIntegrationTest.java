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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** Exercises the Azure adapter against a real Azurite Blob service. */
class AzureStorageClientIntegrationTest {

  private static final int BLOB_PORT = 10000;

  private static final String ACCOUNT = "devstoreaccount1";

  private static final String KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
          + "K1SZFPTOtr/KBHBeksoGMGw==";

  @Test
  void supportsLifecycleStreamingRangesAndSignedCapabilities() throws Exception {
    try (var azurite = azurite()) {
      azurite.start();
      try (var storage = new AzureStorageClient(configuration(azurite))) {
        String container = "object-storage-contract";
        String key = "tenant/evidence/document.txt";
        byte[] content = "provider-neutral evidence".getBytes(StandardCharsets.UTF_8);

        storage.createBucket(new StorageClient.CreateBucketRequest(container, Map.of(), Map.of()));
        assertTrue(storage.bucketExists(container));

        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(
                container, key, content, "text/plain", Map.of("tenant", "contract-test")));

        var head = storage.headObject(new StorageClient.HeadObjectRequest(container, key));
        assertEquals(content.length, head.size());
        assertEquals("contract-test", head.userMetadata().get("tenant"));

        try (var object = storage.getObject(StorageClient.GetObjectRequest.of(container, key))) {
          assertEquals("text/plain", object.metadata().contentType().orElseThrow());
          assertArrayEquals(content, object.content().readAllBytes());
        }

        try (var range =
            storage.getObject(new StorageClient.GetObjectRequest(container, key, 9L, 15L))) {
          assertEquals(
              "neutral", new String(range.content().readAllBytes(), StandardCharsets.UTF_8));
        }

        String uploadKey = "tenant/evidence/signed.txt";
        var putCapability =
            storage.presignPut(
                new StorageClient.PresignPutRequest(
                    container, uploadKey, Duration.ofMinutes(5), "text/plain"));
        var http = HttpClient.newHttpClient();
        var put =
            http.send(
                request(putCapability)
                    .PUT(HttpRequest.BodyPublishers.ofString("signed upload"))
                    .build(),
                HttpResponse.BodyHandlers.ofString());
        assertTrue(put.statusCode() >= 200 && put.statusCode() < 300, put.body());

        var getCapability =
            storage.presignGet(
                new StorageClient.PresignGetRequest(container, uploadKey, Duration.ofMinutes(5)));
        var get =
            http.send(request(getCapability).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(200, get.statusCode());
        assertEquals("signed upload", get.body());

        storage.deleteObject(new StorageClient.DeleteObjectRequest(container, key));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(container, uploadKey));
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(container, true));
        assertFalse(storage.bucketExists(container));
      }
    }
  }

  @Test
  void paginatedListingsFilesStreamsAndRangeBoundariesPreserveObjects() throws Exception {
    try (var azurite = azurite()) {
      azurite.start();
      try (var storage = new AzureStorageClient(configuration(azurite))) {
        String bucket = "pagination-contract";
        storage.createBucket(new StorageClient.CreateBucketRequest(bucket, null, null));
        assertTrue(storage.getBucket(bucket).isPresent());
        assertTrue(storage.listBuckets().stream().anyMatch(value -> value.name().equals(bucket)));
        var file = java.nio.file.Files.createTempFile("azure-storage-contract", ".txt");
        try {
          java.nio.file.Files.writeString(file, "abcdef");
          storage.putObject(
              StorageClient.PutObjectRequest.fromPath(bucket, "a.txt", file, null, null));
          try (var stream =
              new java.io.ByteArrayInputStream("second".getBytes(StandardCharsets.UTF_8))) {
            storage.putObject(
                StorageClient.PutObjectRequest.fromStream(bucket, "b.txt", stream, 6, " ", null));
          }
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
        var first =
            storage.listObjects(new StorageClient.ListObjectsRequest(bucket, "", null, 1, null));
        assertEquals(1, first.objects().size());
        assertTrue(first.isTruncated(), "A limited page must expose its continuation token");
        var second =
            storage.listObjects(
                new StorageClient.ListObjectsRequest(
                    bucket, null, null, 1, first.nextContinuationToken()));
        assertEquals(1, second.objects().size());
        assertFalse(first.objects().getFirst().key().equals(second.objects().getFirst().key()));
        assertFalse(second.isTruncated());
        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(
                bucket, "folder/deep.txt", new byte[] {1}, null, null));
        assertEquals(
            1,
            storage
                .listObjects(new StorageClient.ListObjectsRequest(bucket, "a", " ", 0, " "))
                .objects()
                .size());
        assertEquals(
            2,
            storage
                .listObjects(new StorageClient.ListObjectsRequest(bucket, null, "/", 0, null))
                .objects()
                .size());
        try (var head =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", null, 2L));
            var tail =
                storage.getObject(new StorageClient.GetObjectRequest(bucket, "a.txt", 3L, null));
            var full =
                storage.getObject(
                    new StorageClient.GetObjectRequest(bucket, "a.txt", 0L, Long.MAX_VALUE))) {
          assertEquals(bucket, head.bucketName());
          assertEquals("a.txt", head.key());
          assertEquals("abc", new String(head.content().readAllBytes(), StandardCharsets.UTF_8));
          assertEquals("def", new String(tail.content().readAllBytes(), StandardCharsets.UTF_8));
          assertEquals("abcdef", new String(full.content().readAllBytes(), StandardCharsets.UTF_8));
        }
        assertEquals(
            "CONFLICT",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.createBucket(
                            new StorageClient.CreateBucketRequest(bucket, null, null)))
                .code());
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () ->
                        storage.headObject(new StorageClient.HeadObjectRequest(bucket, "missing")))
                .code());
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () -> storage.getObject(StorageClient.GetObjectRequest.of(bucket, "missing")))
                .code());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
        assertTrue(storage.getBucket(bucket).isEmpty());
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, true));
        storage.deleteBucket(new StorageClient.DeleteBucketRequest(bucket, false));
        storage.deleteObject(new StorageClient.DeleteObjectRequest(bucket, "missing"));
        assertEquals(
            "NOT_FOUND",
            assertThrows(
                    StorageException.class,
                    () -> storage.listObjects(StorageClient.ListObjectsRequest.of(bucket)))
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

  private static GenericContainer<?> azurite() {
    return new GenericContainer<>(
            DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.36.0"))
        .withExposedPorts(BLOB_PORT)
        .withCommand(
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--blobPort",
            Integer.toString(BLOB_PORT),
            "--loose",
            "--skipApiVersionCheck")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(1)));
  }

  private static NamedStorageClientConfig configuration(GenericContainer<?> azurite) {
    String endpoint = "http://" + azurite.getHost() + ':' + azurite.getMappedPort(BLOB_PORT);
    return new Configuration(endpoint);
  }

  private record Configuration(String endpointValue) implements NamedStorageClientConfig {

    @Override
    public String backend() {
      return "azure";
    }

    @Override
    public Optional<String> bucket() {
      return Optional.empty();
    }

    @Override
    public Optional<String> endpoint() {
      return Optional.of(endpointValue);
    }

    @Override
    public Optional<String> publicEndpoint() {
      return Optional.of(endpointValue);
    }

    @Override
    public Optional<String> region() {
      return Optional.empty();
    }

    @Override
    public Optional<String> accessKey() {
      return Optional.of(ACCOUNT);
    }

    @Override
    public Optional<String> secretKey() {
      return Optional.of(KEY);
    }

    @Override
    public Optional<Boolean> pathStyleAccess() {
      return Optional.of(true);
    }

    @Override
    public Map<String, String> properties() {
      return Map.of();
    }
  }

  private static HttpRequest.Builder request(StorageClient.PresignedRequest capability) {
    var builder = HttpRequest.newBuilder(capability.uri());
    capability.requiredHeaders().forEach(builder::header);
    return builder;
  }
}
