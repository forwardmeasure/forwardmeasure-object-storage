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

import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobContainerItem;
import com.azure.storage.blob.models.BlobHttpHeaders;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.forwardmeasure.objectstorage.Bucket;
import com.forwardmeasure.objectstorage.DefaultObjectMetadata;
import com.forwardmeasure.objectstorage.ObjectChecksums;
import com.forwardmeasure.objectstorage.ObjectInfo;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.StorageObject;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class AzureStorageClient implements StorageClient {

  private static final String PROVIDER_ID = "azure";

  private final BlobServiceClient serviceClient;

  private final BlobServiceClient publicServiceClient;
  private final EventLoopGroup eventLoopGroup;

  public AzureStorageClient(NamedStorageClientConfig config) {
    String accountName =
        config
            .accessKey()
            .orElseThrow(
                () ->
                    StorageException.invalidRequest(
                        PROVIDER_ID, "Azure account name is required", null));
    String accountKey =
        config
            .secretKey()
            .orElseThrow(
                () ->
                    StorageException.invalidRequest(
                        PROVIDER_ID, "Azure account key is required", null));

    var credential = new StorageSharedKeyCredential(accountName, accountKey);
    this.eventLoopGroup = new NioEventLoopGroup(2);
    this.serviceClient = buildClient(config, credential, eventLoopGroup);
    this.publicServiceClient =
        config
            .publicEndpoint()
            .filter(endpoint -> !endpoint.equals(config.endpoint().orElse(null)))
            .map(endpoint -> buildClient(endpoint, config, credential, eventLoopGroup))
            .orElse(serviceClient);
  }

  AzureStorageClient(BlobServiceClient serviceClient, EventLoopGroup eventLoopGroup) {
    this.serviceClient = serviceClient;
    this.publicServiceClient = serviceClient;
    this.eventLoopGroup = eventLoopGroup;
  }

  @Override
  public List<Bucket> listBuckets() {
    try {
      List<Bucket> out = new ArrayList<>();
      for (BlobContainerItem item : serviceClient.listBlobContainers()) {
        out.add(new Bucket(item.getName(), null, null, Map.of(), Map.of()));
      }
      return out;
    } catch (BlobStorageException e) {
      throw mapException("Failed to list buckets", e);
    }
  }

  @Override
  public Bucket createBucket(CreateBucketRequest request) {
    try {
      BlobContainerClient container = serviceClient.createBlobContainer(request.bucketName());
      return new Bucket(
          container.getBlobContainerName(), null, null, request.tags(), request.attributes());
    } catch (BlobStorageException e) {
      if (e.getStatusCode() == 409) {
        throw StorageException.conflict(
            PROVIDER_ID, "Bucket already exists: " + request.bucketName(), e);
      }
      throw mapException("Failed to create bucket: " + request.bucketName(), e);
    }
  }

  @Override
  public void deleteBucket(DeleteBucketRequest request) {
    try {
      BlobContainerClient container = container(request.bucketName());
      if (request.forceDeleteContents() && container.exists()) {
        for (BlobItem blob : container.listBlobs()) {
          container.getBlobClient(blob.getName()).deleteIfExists();
        }
      }
      container.deleteIfExists();
    } catch (BlobStorageException e) {
      throw mapException("Failed to delete bucket: " + request.bucketName(), e);
    }
  }

  @Override
  public boolean bucketExists(String bucketName) {
    try {
      return container(bucketName).exists();
    } catch (BlobStorageException e) {
      throw mapException("Failed to check bucket existence: " + bucketName, e);
    }
  }

  @Override
  public Optional<Bucket> getBucket(String bucketName) {
    try {
      BlobContainerClient container = container(bucketName);
      if (!container.exists()) {
        return Optional.empty();
      }
      return Optional.of(new Bucket(bucketName, null, null, Map.of(), Map.of()));
    } catch (BlobStorageException e) {
      throw mapException("Failed to get bucket: " + bucketName, e);
    }
  }

  @Override
  public ListObjectsResponse listObjects(ListObjectsRequest request) {
    return listObjects(request, false);
  }

  @Override
  public ListObjectsResponse listObjectVersions(ListObjectsRequest request) {
    if (request.delimiter() != null && !request.delimiter().isBlank())
      throw new IllegalArgumentException("Version listings do not support delimiter grouping");
    return listObjects(request, true);
  }

  private ListObjectsResponse listObjects(ListObjectsRequest request, boolean versions) {
    try {
      BlobContainerClient container = container(request.bucketName());
      if (!container.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID, "Bucket not found: " + request.bucketName(), null);
      }

      ListBlobsOptions options = new ListBlobsOptions();
      if (versions)
        options.setDetails(
            new com.azure.storage.blob.models.BlobListDetails()
                .setRetrieveVersions(true)
                .setRetrieveMetadata(true));
      if (request.prefix() != null && !request.prefix().isBlank()) {
        options.setPrefix(request.prefix());
      }
      if (request.maxKeys() > 0) {
        options.setMaxResultsPerPage(request.maxKeys());
      }

      var listing =
          request.delimiter() == null || request.delimiter().isBlank()
              ? container.listBlobs(options, null)
              : container.listBlobsByHierarchy(request.delimiter(), options, null);
      String token = request.continuationToken();
      var pages =
          (token == null || token.isBlank()
                  ? listing.iterableByPage()
                  : listing.iterableByPage(token))
              .iterator();
      if (!pages.hasNext()) {
        return new ListObjectsResponse(List.of(), null, false);
      }
      var page = pages.next();

      List<ObjectInfo> out = new ArrayList<>();
      for (BlobItem blob : page.getValue()) {
        if (Boolean.TRUE.equals(blob.isPrefix())) {
          continue;
        }
        var listedBlob = container.getBlobClient(blob.getName());
        if (versions && blob.getVersionId() != null)
          listedBlob = listedBlob.getVersionClient(blob.getVersionId());
        BlobProperties p;
        try {
          p = listedBlob.getProperties();
        } catch (BlobStorageException removed) {
          if (removed.getStatusCode() == 404) continue;
          throw removed;
        }
        out.add(
            new ObjectInfo(
                request.bucketName(),
                blob.getName(),
                p.getBlobSize(),
                p.getLastModified() == null ? null : p.getLastModified().toInstant(),
                p.getETag(),
                versions ? p.getVersionId() : null,
                null,
                p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
                Map.of()));
      }

      String next = page.getContinuationToken();
      return new ListObjectsResponse(out, next, next != null && !next.isBlank());
    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to list objects: " + request.bucketName(), e);
    }
  }

  @Override
  public ObjectInfo headObject(HeadObjectRequest request) {
    try {
      BlobClient blob = blob(request.bucketName(), request.key(), request.versionId());
      if (!blob.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID,
            "Object not found: "
                + describe(request.bucketName(), request.key(), request.versionId()),
            null);
      }

      BlobProperties p = blob.getProperties();
      return new ObjectInfo(
          request.bucketName(),
          request.key(),
          p.getBlobSize(),
          p.getLastModified() == null ? null : p.getLastModified().toInstant(),
          p.getETag(),
          p.getVersionId(),
          null,
          p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
          Map.of(),
          checksums(p));
    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to head object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public StorageObject getObject(GetObjectRequest request) {
    try {
      BlobClient blob = blob(request.bucketName(), request.key(), request.versionId());
      if (!blob.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID,
            "Object not found: "
                + describe(request.bucketName(), request.key(), request.versionId()),
            null);
      }

      BlobProperties p = blob.getProperties();

      // Read exactly the content the metadata describes: the read fails rather than returning a
      // blob overwritten since the properties were fetched.
      BlobInputStreamOptions options =
          new BlobInputStreamOptions()
              .setRequestConditions(new BlobRequestConditions().setIfMatch(p.getETag()));
      if (request.rangeStartInclusive() != null || request.rangeEndInclusive() != null) {
        long start = request.rangeStartInclusive() == null ? 0L : request.rangeStartInclusive();
        Long end = request.rangeEndInclusive();
        Long count = end == null || end - start == Long.MAX_VALUE ? null : end - start + 1;
        options.setRange(new BlobRange(start, count));
      }
      InputStream stream = blob.openInputStream(options);

      DefaultObjectMetadata md =
          new DefaultObjectMetadata(
              p.getContentType(),
              p.getBlobSize(),
              p.getETag(),
              p.getVersionId(),
              p.getLastModified() == null ? null : p.getLastModified().toInstant(),
              p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
              Map.of(),
              Map.of(),
              p.getContentEncoding(),
              p.getCacheControl(),
              p.getContentDisposition(),
              p.getContentLanguage(),
              checksums(p));

      return new AzureStorageObject(request.bucketName(), request.key(), md, stream);
    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to get object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest request) {
    try {
      BlobContainerClient container = container(request.bucketName());
      if (!container.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID, "Bucket not found: " + request.bucketName(), null);
      }

      BlobClient blob = container.getBlobClient(request.key());

      BlobHttpHeaders headers = new BlobHttpHeaders();
      if (request.contentType() != null && !request.contentType().isBlank()) {
        headers.setContentType(request.contentType());
      }

      Map<String, String> metadata =
          request.userMetadata() == null ? Map.of() : request.userMetadata();

      // Commit bytes, headers and ownership metadata together. Post-upload header/metadata writes
      // create extra content-bearing versions on versioned Azure accounts, outside the returned
      // locator and therefore outside exact-version cleanup.
      {
        InputStream content;
        long length;
        if (request.bytes() != null) {
          headers.setContentMd5(
              java.security.MessageDigest.getInstance("MD5").digest(request.bytes()));
          content = new ByteArrayInputStream(request.bytes());
          length = request.bytes().length;
        } else if (request.stream() != null) {
          // The SDK consumes this stream once, buffers retryable blocks, then commits the block
          // list with these headers. Finalize the whole-object MD5 at EOF, before that commit.
          // The protocol fixture covers both single-PUT and multi-block uploads: changing headers
          // afterwards would create another content version and is deliberately forbidden.
          content = new UploadStream(request.stream(), request.contentLength(), headers);
          length = request.contentLength();
        } else {
          java.nio.file.Path path = request.filePath();
          if (path == null)
            throw new IllegalArgumentException(
                "Exactly one of filePath, bytes, or stream must be provided");
          var digest = java.security.MessageDigest.getInstance("MD5");
          try (var in =
              new java.security.DigestInputStream(
                  java.nio.file.Files.newInputStream(path), digest)) {
            in.transferTo(java.io.OutputStream.nullOutputStream());
          }
          headers.setContentMd5(digest.digest());
          length = java.nio.file.Files.size(path);
          content = java.nio.file.Files.newInputStream(path);
        }
        try (content) {
          var uploaded =
              blob.uploadWithResponse(
                      new BlobParallelUploadOptions(content)
                          .setParallelTransferOptions(
                              new com.azure.storage.blob.models.ParallelTransferOptions()
                                  .setBlockSizeLong(
                                      Math.max(
                                          4L * 1024 * 1024,
                                          length == 0
                                              ? 0
                                              : 1
                                                  + (length - 1)
                                                      / com.azure.storage.blob.specialized
                                                          .BlockBlobClient.MAX_BLOCKS))
                                  .setMaxSingleUploadSizeLong(4L * 1024 * 1024)
                                  .setMaxConcurrency(2))
                          .setComputeMd5(true)
                          .setHeaders(headers)
                          .setMetadata(metadata),
                      null,
                      com.azure.core.util.Context.NONE)
                  .getValue();
          // Use this write's response; a subsequent HEAD might describe a concurrent replacement.
          return new PutObjectResult(
              request.bucketName(), request.key(), uploaded.getETag(), uploaded.getVersionId());
        }
      }

    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to put object: " + request.bucketName() + "/" + request.key(), e);
    } catch (Exception e) {
      throw StorageException.internal(
          PROVIDER_ID, "Failed to put object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  /** One forward pass over a caller-owned stream; no plaintext staging or ownership transfer. */
  private static final class UploadStream extends java.io.FilterInputStream {
    private final long length;
    private final BlobHttpHeaders headers;
    private final java.security.MessageDigest digest;
    private long count;
    private boolean complete;

    private UploadStream(InputStream input, long length, BlobHttpHeaders headers)
        throws java.security.NoSuchAlgorithmException {
      super(input);
      this.length = length;
      this.headers = headers;
      this.digest = java.security.MessageDigest.getInstance("MD5");
    }

    @Override
    public int read() throws java.io.IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) == -1 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] bytes, int offset, int requested) throws java.io.IOException {
      java.util.Objects.checkFromIndexSize(offset, requested, bytes.length);
      if (requested == 0) return 0;
      if (complete) return -1;
      if (count == length) {
        if (in.read() != -1)
          throw new java.io.IOException("Upload stream exceeds its declared length");
        headers.setContentMd5(digest.digest());
        complete = true;
        return -1;
      }
      int received = in.read(bytes, offset, (int) Math.min(requested, length - count));
      if (received < 0)
        throw new java.io.IOException("Upload stream is shorter than its declared length");
      digest.update(bytes, offset, received);
      count += received;
      return received;
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    @Override
    public void reset() throws java.io.IOException {
      throw new java.io.IOException("Upload streams cannot be replayed");
    }

    @Override
    public void close() {
      // The caller retains ownership on success and failure.
    }
  }

  @Override
  public void deleteObject(DeleteObjectRequest request) {
    try {
      blob(request.bucketName(), request.key(), request.versionId()).deleteIfExists();
    } catch (BlobStorageException e) {
      throw mapException(
          "Failed to delete object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public PresignedRequest presignGet(PresignGetRequest request) {
    try {
      BlobClient blob = publicBlob(request.bucketName(), request.key());
      OffsetDateTime expiry = OffsetDateTime.now().plus(request.expires());
      BlobSasPermission permissions = new BlobSasPermission().setReadPermission(true);
      BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiry, permissions);
      String sas = blob.generateSas(values);
      return new PresignedRequest(java.net.URI.create(blob.getBlobUrl() + "?" + sas), Map.of());
    } catch (BlobStorageException e) {
      throw mapException("Failed to presign GET: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public PresignedRequest presignPut(PresignPutRequest request) {
    try {
      BlobClient blob = publicBlob(request.bucketName(), request.key());
      OffsetDateTime expiry = OffsetDateTime.now().plus(request.expires());
      BlobSasPermission permissions =
          new BlobSasPermission().setCreatePermission(true).setWritePermission(true);
      BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiry, permissions);
      if (request.contentType() != null && !request.contentType().isBlank()) {
        values.setContentType(request.contentType());
      }
      String sas = blob.generateSas(values);
      Map<String, String> headers = new java.util.LinkedHashMap<>();
      headers.put("x-ms-blob-type", "BlockBlob");
      if (request.contentType() != null && !request.contentType().isBlank()) {
        headers.put("content-type", request.contentType());
      }
      return new PresignedRequest(java.net.URI.create(blob.getBlobUrl() + "?" + sas), headers);
    } catch (BlobStorageException e) {
      throw mapException("Failed to presign PUT: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public Object unwrap() {
    return serviceClient;
  }

  @Override
  public void close() {
    eventLoopGroup.shutdownGracefully();
  }

  private BlobContainerClient container(String bucketName) {
    return serviceClient.getBlobContainerClient(bucketName);
  }

  private BlobClient blob(String bucketName, String key) {
    return container(bucketName).getBlobClient(key);
  }

  private BlobClient blob(String bucketName, String key, String versionId) {
    BlobClient blob = blob(bucketName, key);
    return versionId == null ? blob : blob.getVersionClient(versionId);
  }

  private static String describe(String bucket, String key, String versionId) {
    return bucket + "/" + key + (versionId == null ? "" : "#" + versionId);
  }

  private static ObjectChecksums checksums(BlobProperties properties) {
    byte[] md5 = properties.getContentMd5();
    return new ObjectChecksums(
        null, md5 == null ? null : java.util.Base64.getEncoder().encodeToString(md5));
  }

  private BlobClient publicBlob(String bucketName, String key) {
    return publicServiceClient.getBlobContainerClient(bucketName).getBlobClient(key);
  }

  private static BlobServiceClient buildClient(
      NamedStorageClientConfig cfg,
      StorageSharedKeyCredential credential,
      EventLoopGroup eventLoopGroup) {

    String accountName = cfg.accessKey().orElseThrow();
    String endpoint = cfg.endpoint().orElse("https://" + accountName + ".blob.core.windows.net");

    return buildClient(endpoint, cfg, credential, eventLoopGroup);
  }

  private static BlobServiceClient buildClient(
      String endpoint,
      NamedStorageClientConfig cfg,
      StorageSharedKeyCredential credential,
      EventLoopGroup eventLoopGroup) {

    String accountName = cfg.accessKey().orElseThrow();

    String normalized =
        endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
    if (!normalized.contains("/" + accountName) && !normalized.contains(".blob.core.windows.net")) {
      normalized = normalized + "/" + accountName;
    }

    return new BlobServiceClientBuilder()
        .endpoint(normalized)
        .credential(credential)
        .httpClient(new NettyAsyncHttpClientBuilder().eventLoopGroup(eventLoopGroup).build())
        .buildClient();
  }

  private StorageException mapException(String message, BlobStorageException e) {
    int code = e.getStatusCode();
    if (code == 404) {
      return StorageException.notFound(PROVIDER_ID, message, e);
    }
    if (code == 403) {
      return StorageException.accessDenied(PROVIDER_ID, message, e);
    }
    if (code == 409) {
      return StorageException.conflict(PROVIDER_ID, message, e);
    }
    if (code == 429 || code == 500 || code == 502 || code == 503 || code == 504) {
      return StorageException.retryable(PROVIDER_ID, message, e);
    }
    return StorageException.internal(PROVIDER_ID, message, e);
  }

  private static final class AzureStorageObject implements StorageObject {
    private final String bucket;
    private final String key;
    private final DefaultObjectMetadata metadata;
    private final InputStream stream;

    private AzureStorageObject(
        String bucket, String key, DefaultObjectMetadata metadata, InputStream stream) {
      this.bucket = bucket;
      this.key = key;
      this.metadata = metadata;
      this.stream = stream;
    }

    @Override
    public String bucketName() {
      return bucket;
    }

    @Override
    public String key() {
      return key;
    }

    @Override
    public DefaultObjectMetadata metadata() {
      return metadata;
    }

    @Override
    public InputStream content() {
      return stream;
    }

    @Override
    public void close() {
      try {
        stream.close();
      } catch (Exception ignored) {
      }
    }
  }
}
