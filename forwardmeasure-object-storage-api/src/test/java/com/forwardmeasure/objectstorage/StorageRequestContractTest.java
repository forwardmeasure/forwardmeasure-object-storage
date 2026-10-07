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
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.junit.jupiter.api.Test;

class StorageRequestContractTest {
  @Test
  void objectOperationsRejectMissingBucketOrKeyBeforeCallingAProvider() {
    List<BiFunction<String, String, Object>> operations =
        List.of(
            StorageClient.HeadObjectRequest::new,
            StorageClient.GetObjectRequest::of,
            StorageClient.DeleteObjectRequest::new,
            (bucket, key) ->
                StorageClient.PutObjectRequest.fromBytes(bucket, key, new byte[0], null, null),
            (bucket, key) ->
                new StorageClient.PresignGetRequest(bucket, key, Duration.ofMinutes(1)),
            (bucket, key) ->
                new StorageClient.PresignPutRequest(
                    bucket, key, Duration.ofMinutes(1), "text/plain"),
            (bucket, key) -> new ObjectInfo(bucket, key, 0, null, null, null, null, null, null));
    for (var operation : operations) {
      operation.apply("bucket", "nested/key");
      for (String missing : new String[] {null, "", " "}) {
        assertThrows(IllegalArgumentException.class, () -> operation.apply(missing, "key"));
        assertThrows(IllegalArgumentException.class, () -> operation.apply("bucket", missing));
      }
    }
    for (String missing : new String[] {null, "", " "}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new StorageClient.CreateBucketRequest(missing, null, null));
      assertThrows(
          IllegalArgumentException.class,
          () -> new StorageClient.DeleteBucketRequest(missing, false));
      assertThrows(
          IllegalArgumentException.class, () -> StorageClient.ListObjectsRequest.of(missing));
    }
    assertEquals("bucket", new StorageClient.DeleteBucketRequest("bucket", true).bucketName());
    assertEquals(0, StorageClient.ListObjectsRequest.of("bucket").maxKeys());
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClient.ListObjectsRequest("bucket", null, null, -1, null));
  }

  @Test
  void uploadRequiresExactlyOneSourceAndKnownLengthForStreams() {
    byte[] bytes = {1, 2, 3};
    var metadata = new HashMap<>(Map.of("source", "vendor"));
    var upload =
        StorageClient.PutObjectRequest.fromBytes(
            "bucket", "key", bytes, "application/octet-stream", metadata);
    metadata.clear();
    assertArrayEquals(bytes, upload.bytes());
    assertEquals(Map.of("source", "vendor"), upload.userMetadata());
    assertEquals(
        Path.of("source.bin"),
        StorageClient.PutObjectRequest.fromPath("bucket", "key", Path.of("source.bin"), null, null)
            .filePath());
    var stream = new ByteArrayInputStream(bytes);
    var streamed =
        StorageClient.PutObjectRequest.fromStream("bucket", "key", stream, 3, null, null);
    assertSame(stream, streamed.stream());
    assertEquals(3L, streamed.contentLength());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageClient.PutObjectRequest(
                "bucket", "key", null, null, null, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageClient.PutObjectRequest(
                "bucket", "key", Path.of("file"), bytes, null, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageClient.PutObjectRequest(
                "bucket", "key", null, bytes, stream, 3L, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StorageClient.PutObjectRequest(
                "bucket", "key", null, null, stream, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> StorageClient.PutObjectRequest.fromStream("bucket", "key", stream, -1, null, null));
  }

  @Test
  void rangesPreserveOpenBoundsAndRejectNegativeOrInvertedOffsets() {
    assertEquals(
        5L, new StorageClient.GetObjectRequest("bucket", "key", 5L, null).rangeStartInclusive());
    assertEquals(
        5L, new StorageClient.GetObjectRequest("bucket", "key", null, 5L).rangeEndInclusive());
    assertEquals(
        5L, new StorageClient.GetObjectRequest("bucket", "key", 5L, 5L).rangeEndInclusive());
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClient.GetObjectRequest("bucket", "key", -1L, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClient.GetObjectRequest("bucket", "key", null, -1L));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClient.GetObjectRequest("bucket", "key", 6L, 5L));
  }

  @Test
  void presignedCapabilitiesRequirePositiveExpiryAndImmutableExactHeaders() {
    for (Duration invalid : new Duration[] {null, Duration.ZERO, Duration.ofSeconds(-1)}) {
      assertThrows(
          IllegalArgumentException.class,
          () -> new StorageClient.PresignGetRequest("bucket", "key", invalid));
      assertThrows(
          IllegalArgumentException.class,
          () -> new StorageClient.PresignPutRequest("bucket", "key", invalid, null));
    }
    assertThrows(
        IllegalArgumentException.class, () -> new StorageClient.PresignedRequest(null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new StorageClient.PresignedRequest(URI.create("relative"), null));
    var headers = new HashMap<>(Map.of("Content-Type", "application/json"));
    var capability =
        new StorageClient.PresignedRequest(URI.create("https://storage.example/object"), headers);
    headers.clear();
    assertEquals(Map.of("Content-Type", "application/json"), capability.requiredHeaders());
    assertThrows(UnsupportedOperationException.class, () -> capability.requiredHeaders().clear());
    assertTrue(
        new StorageClient.PresignedRequest(URI.create("https://storage.example/object"), null)
            .requiredHeaders()
            .isEmpty());
  }

  @Test
  void metadataSnapshotsPreserveProviderValuesAndUnknownValuesRemainAbsent() {
    var map = new HashMap<>(Map.of("key", "value"));
    var now = Instant.parse("2026-10-07T00:00:00Z");
    var metadata =
        new DefaultObjectMetadata(
            "text/plain",
            3,
            "tag",
            "version",
            now,
            map,
            map,
            map,
            "gzip",
            "no-cache",
            "attachment",
            "en");
    var bucket = new Bucket("bucket", now, "region", map, map);
    var create = new StorageClient.CreateBucketRequest("bucket", map, map);
    var info = new ObjectInfo("bucket", "key", 3, now, "tag", "version", "standard", map, map);
    map.clear();
    assertEquals(Map.of("key", "value"), metadata.userMetadata());
    assertEquals(metadata.userMetadata(), metadata.systemMetadata());
    assertEquals(metadata.userMetadata(), metadata.attributes());
    assertEquals(metadata.userMetadata(), bucket.tags());
    assertEquals(metadata.userMetadata(), bucket.attributes());
    assertEquals(metadata.userMetadata(), create.tags());
    assertEquals(metadata.userMetadata(), info.userMetadata());
    assertEquals(now, bucket.createdAtOptional().orElseThrow());
    assertEquals("region", bucket.locationOptional().orElseThrow());
    assertEquals(3, metadata.contentLength());
    assertEquals(
        List.of("text/plain", "tag", "version", "gzip", "no-cache", "attachment", "en"),
        List.of(
            metadata.contentType().orElseThrow(),
            metadata.contentTag().orElseThrow(),
            metadata.versionId().orElseThrow(),
            metadata.contentEncoding().orElseThrow(),
            metadata.cacheControl().orElseThrow(),
            metadata.contentDisposition().orElseThrow(),
            metadata.contentLanguage().orElseThrow()));
    assertEquals(now, metadata.lastModified().orElseThrow());
    assertEquals(now, info.lastModifiedOptional().orElseThrow());
    assertEquals("tag", info.contentTagOptional().orElseThrow());
    assertEquals("version", info.versionIdOptional().orElseThrow());
    assertEquals("standard", info.storageClassOptional().orElseThrow());
    var unknown =
        new DefaultObjectMetadata(
            null, 0, null, null, null, null, null, null, null, null, null, null);
    assertTrue(unknown.contentType().isEmpty());
    assertTrue(unknown.userMetadata().isEmpty());
    assertTrue(new Bucket("bucket", null, null, null, null).tags().isEmpty());
    assertTrue(new StorageClient.CreateBucketRequest("bucket", null, null).attributes().isEmpty());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DefaultObjectMetadata(
                null, -1, null, null, null, null, null, null, null, null, null, null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ObjectInfo("bucket", "key", -1, null, null, null, null, null, null));
    var objects = new java.util.ArrayList<>(List.of(info));
    var page = new StorageClient.ListObjectsResponse(objects, "next", true);
    objects.clear();
    assertEquals(List.of(info), page.objects());
    assertFalse(new StorageClient.ListObjectsResponse(null, null, false).isTruncated());
  }
}
