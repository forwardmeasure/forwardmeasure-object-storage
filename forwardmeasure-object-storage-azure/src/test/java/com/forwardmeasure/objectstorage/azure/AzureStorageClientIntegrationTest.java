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

  /**
   * Objects carry their MD5, and a read is pinned to the blob the metadata describes: overwritten
   * after it was opened, it either still yields exactly the original bytes or fails - never the
   * replacement's.
   */
  @Test
  void reportsChecksumsAndNeverReadsAReplacedBlob() throws Exception {
    try (var azurite = azurite()) {
      azurite.start();
      try (var storage = new AzureStorageClient(configuration(azurite))) {
        String bucket = "object-storage-pinning";
        String key = "sources/feed.csv";
        byte[] first = "uid,name\n1,First\n".getBytes(StandardCharsets.UTF_8);
        byte[] second = "uid,name\n2,Second\n".getBytes(StandardCharsets.UTF_8);
        storage.createBucket(new StorageClient.CreateBucketRequest(bucket, Map.of(), Map.of()));
        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(bucket, key, first, "text/csv", Map.of()));

        var head = storage.headObject(new StorageClient.HeadObjectRequest(bucket, key));
        assertEquals(
            java.util.Base64.getEncoder()
                .encodeToString(java.security.MessageDigest.getInstance("MD5").digest(first)),
            head.checksums().md5());

        var opened = storage.getObject(StorageClient.GetObjectRequest.of(bucket, key));
        storage.putObject(
            StorageClient.PutObjectRequest.fromBytes(bucket, key, second, "text/csv", Map.of()));
        try {
          assertArrayEquals(first, opened.content().readAllBytes());
        } catch (java.io.IOException | RuntimeException replaced) {
          // The read was refused because the blob changed: equally correct.
        } finally {
          opened.close();
        }
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

  private record UploadCommit(String query, String owner, String md5) {}

  /**
   * Azurite proves the write protocol; Azure version retention is checked by the opt-in account
   * fixture.
   */
  @Test
  void bytesFilesAndBorrowedStreamsCommitHeadersAndOwnershipWithoutExtraMutationRequests()
      throws Exception {
    try (var azurite = azurite()) {
      azurite.start();
      var commits = new java.util.concurrent.CopyOnWriteArrayList<UploadCommit>();
      var loops = new io.netty.channel.nio.NioEventLoopGroup(2);
      var sdk =
          new com.azure.storage.blob.BlobServiceClientBuilder()
              .endpoint(configuration(azurite).endpoint().orElseThrow() + "/" + ACCOUNT)
              .credential(new com.azure.storage.common.StorageSharedKeyCredential(ACCOUNT, KEY))
              .httpClient(
                  new com.azure.core.http.netty.NettyAsyncHttpClientBuilder()
                      .eventLoopGroup(loops)
                      .build())
              .addPolicy(
                  new com.azure.core.http.policy.HttpPipelinePolicy() {
                    @Override
                    public reactor.core.publisher.Mono<com.azure.core.http.HttpResponse> process(
                        com.azure.core.http.HttpPipelineCallContext context,
                        com.azure.core.http.HttpPipelineNextPolicy next) {
                      var request = context.getHttpRequest();
                      if (request.getHttpMethod() == com.azure.core.http.HttpMethod.PUT
                          && request.getUrl().getPath().contains("/atomic-"))
                        commits.add(
                            new UploadCommit(
                                request.getUrl().getQuery(),
                                request.getHeaders().getValue("x-ms-meta-owner"),
                                request.getHeaders().getValue("x-ms-blob-content-md5")));
                      return next.process();
                    }
                  })
              .buildClient();
      try (var storage = new AzureStorageClient(sdk, loops)) {
        String bucket = "atomic-archive-upload";
        storage.createBucket(new StorageClient.CreateBucketRequest(bucket, null, null));
        var file = java.nio.file.Files.createTempFile("azure-atomic-upload", ".txt");
        try {
          for (int length : new int[] {0, 23, 8 * 1024 * 1024 + 17}) {
            byte[] bytes = new byte[length];
            new java.util.Random(42).nextBytes(bytes);
            java.nio.file.Files.write(file, bytes);
            for (int source = 0; source < 3; source++) {
              commits.clear();
              String key = "atomic-" + source;
              var closed = new java.util.concurrent.atomic.AtomicBoolean();
              var borrowed =
                  new java.io.ByteArrayInputStream(bytes) {
                    @Override
                    public void close() throws java.io.IOException {
                      closed.set(true);
                      super.close();
                    }
                  };
              var request =
                  switch (source) {
                    case 0 ->
                        StorageClient.PutObjectRequest.fromBytes(
                            bucket,
                            key,
                            bytes,
                            "application/gzip",
                            Map.of("owner", "archive-operation"));
                    case 1 ->
                        StorageClient.PutObjectRequest.fromPath(
                            bucket,
                            key,
                            file,
                            "application/gzip",
                            Map.of("owner", "archive-operation"));
                    default ->
                        StorageClient.PutObjectRequest.fromStream(
                            bucket,
                            key,
                            borrowed,
                            bytes.length,
                            "application/gzip",
                            Map.of("owner", "archive-operation"));
                  };
              var receipt = storage.putObject(request);
              assertFalse(closed.get(), "The adapter borrows caller streams");
              var contentCommits =
                  commits.stream()
                      .filter(
                          value ->
                              value.query() == null || value.query().contains("comp=blocklist"))
                      .toList();
              assertEquals(1, contentCommits.size(), commits.toString());
              assertFalse(
                  commits.stream()
                      .anyMatch(
                          value ->
                              value.query() != null
                                  && (value.query().contains("comp=metadata")
                                      || value.query().contains("comp=properties"))),
                  commits.toString());
              var commit = contentCommits.getFirst();
              assertEquals("archive-operation", commit.owner());
              if (length > 4 * 1024 * 1024)
                assertTrue(commit.query().contains("comp=blocklist"), commits.toString());
              assertEquals(
                  java.util.Base64.getEncoder()
                      .encodeToString(java.security.MessageDigest.getInstance("MD5").digest(bytes)),
                  commit.md5());
              var head = storage.headObject(new StorageClient.HeadObjectRequest(bucket, key));
              assertEquals(receipt.etag(), head.contentTag());
              assertEquals("archive-operation", head.userMetadata().get("owner"));
              assertEquals(commit.md5(), head.checksums().md5());
              try (var read = storage.getObject(StorageClient.GetObjectRequest.of(bucket, key))) {
                assertArrayEquals(bytes, read.content().readAllBytes());
              }
            }
          }
          for (long declared : new long[] {2, 4}) {
            commits.clear();
            var closed = new java.util.concurrent.atomic.AtomicBoolean();
            var borrowed =
                new java.io.ByteArrayInputStream(new byte[] {1, 2, 3}) {
                  @Override
                  public void close() {
                    closed.set(true);
                  }
                };
            String invalidKey = "atomic-invalid-" + declared;
            assertThrows(
                StorageException.class,
                () ->
                    storage.putObject(
                        StorageClient.PutObjectRequest.fromStream(
                            bucket,
                            invalidKey,
                            borrowed,
                            declared,
                            "application/gzip",
                            Map.of("owner", "invalid"))));
            assertFalse(closed.get(), "Invalid uploads also retain caller stream ownership");
            assertFalse(
                commits.stream()
                    .anyMatch(
                        value -> value.query() == null || value.query().contains("comp=blocklist")),
                commits.toString());
            assertThrows(
                StorageException.class,
                () -> storage.headObject(new StorageClient.HeadObjectRequest(bucket, invalidKey)));
          }
        } finally {
          java.nio.file.Files.deleteIfExists(file);
        }
      }
    }
  }

  /**
   * Requires a disposable real Azure account with blob versioning enabled and soft delete disabled.
   */
  @Test
  @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(
      named = "FORWARDMEASURE_AZURE_VERSIONED_TESTS",
      matches = "true")
  void exactVersionCleanupRemovesTheOnlyContentVersionAndPreservesAnOverwrittenReplacement() {
    var loops = new io.netty.channel.nio.NioEventLoopGroup(2);
    var sdk =
        new com.azure.storage.blob.BlobServiceClientBuilder()
            .endpoint(
                java.util.Objects.requireNonNull(
                    System.getenv("FORWARDMEASURE_AZURE_TEST_ENDPOINT")))
            .credential(
                new com.azure.storage.common.StorageSharedKeyCredential(
                    java.util.Objects.requireNonNull(
                        System.getenv("FORWARDMEASURE_AZURE_TEST_ACCOUNT")),
                    java.util.Objects.requireNonNull(
                        System.getenv("FORWARDMEASURE_AZURE_TEST_ACCOUNT_KEY"))))
            .httpClient(
                new com.azure.core.http.netty.NettyAsyncHttpClientBuilder()
                    .eventLoopGroup(loops)
                    .build())
            .buildClient();
    String bucket = "archive-version-" + java.util.UUID.randomUUID();
    var container = sdk.getBlobContainerClient(bucket);
    container.create();
    try (var storage = new AzureStorageClient(sdk, loops)) {
      var options =
          new com.azure.storage.blob.models.ListBlobsOptions()
              .setDetails(
                  new com.azure.storage.blob.models.BlobListDetails()
                      .setRetrieveVersions(true)
                      .setRetrieveDeletedBlobs(true)
                      .setRetrieveDeletedBlobsWithVersions(true));
      var first =
          storage.putObject(
              StorageClient.PutObjectRequest.fromBytes(
                  bucket,
                  "part",
                  new byte[] {1, 2, 3},
                  "application/gzip",
                  Map.of("owner", "original")));
      assertTrue(
          first.versionId() != null && !first.versionId().isBlank(),
          "The selected account must have versioning enabled");
      assertEquals(1, container.listBlobs(options, null).stream().count());
      var replacement =
          storage.putObject(
              StorageClient.PutObjectRequest.fromBytes(
                  bucket,
                  "part",
                  new byte[] {4, 5, 6},
                  "application/gzip",
                  Map.of("owner", "replacement")));
      assertEquals(2, container.listBlobs(options, null).stream().count());
      var listed = new java.util.ArrayList<com.forwardmeasure.objectstorage.ObjectInfo>();
      String token = null;
      do {
        var page =
            storage.listObjectVersions(
                new StorageClient.ListObjectsRequest(bucket, "part", null, 1, token));
        listed.addAll(page.objects());
        token = page.nextContinuationToken();
      } while (token != null);
      assertEquals(
          java.util.Set.of(first.versionId(), replacement.versionId()),
          listed.stream()
              .map(com.forwardmeasure.objectstorage.ObjectInfo::versionId)
              .collect(java.util.stream.Collectors.toSet()));
      storage.deleteObject(
          new StorageClient.DeleteObjectRequest(bucket, "part", first.versionId()));
      var remaining = container.listBlobs(options, null).stream().toList();
      assertEquals(1, remaining.size());
      assertEquals(replacement.versionId(), remaining.getFirst().getVersionId());
      assertFalse(container.getBlobClient("part").getVersionClient(first.versionId()).exists());
      assertTrue(
          container.getBlobClient("part").getVersionClient(replacement.versionId()).exists());
      storage.deleteObject(
          new StorageClient.DeleteObjectRequest(bucket, "part", replacement.versionId()));
      assertEquals(
          0,
          container.listBlobs(options, null).stream().count(),
          "Exact cleanup must also remove the sole current version and leave no soft-deleted"
              + " bytes");
    } finally {
      container.delete();
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
