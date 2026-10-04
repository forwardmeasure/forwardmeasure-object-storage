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
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.forwardmeasure.objectstorage.Bucket;
import com.forwardmeasure.objectstorage.DefaultObjectMetadata;
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
  private final StorageSharedKeyCredential credential;
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

    this.credential = new StorageSharedKeyCredential(accountName, accountKey);
    this.eventLoopGroup = new NioEventLoopGroup(2);
    this.serviceClient = buildClient(config, credential, eventLoopGroup);
    this.publicServiceClient =
        config
            .publicEndpoint()
            .filter(endpoint -> !endpoint.equals(config.endpoint().orElse(null)))
            .map(endpoint -> buildClient(endpoint, config, credential, eventLoopGroup))
            .orElse(serviceClient);
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
    try {
      BlobContainerClient container = container(request.bucketName());
      if (!container.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID, "Bucket not found: " + request.bucketName(), null);
      }

      ListBlobsOptions options = new ListBlobsOptions();
      if (request.prefix() != null && !request.prefix().isBlank()) {
        options.setPrefix(request.prefix());
      }

      List<ObjectInfo> out = new ArrayList<>();
      for (BlobItem blob : container.listBlobs(options, null)) {
        BlobProperties p = container.getBlobClient(blob.getName()).getProperties();
        out.add(
            new ObjectInfo(
                request.bucketName(),
                blob.getName(),
                p.getBlobSize(),
                p.getLastModified() == null ? null : p.getLastModified().toInstant(),
                p.getETag(),
                null,
                null,
                p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
                Map.of()));

        if (request.maxKeys() > 0 && out.size() >= request.maxKeys()) {
          break;
        }
      }

      return new ListObjectsResponse(out, null, false);
    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to list objects: " + request.bucketName(), e);
    }
  }

  @Override
  public ObjectInfo headObject(HeadObjectRequest request) {
    try {
      BlobClient blob = blob(request.bucketName(), request.key());
      if (!blob.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID, "Object not found: " + request.bucketName() + "/" + request.key(), null);
      }

      BlobProperties p = blob.getProperties();
      return new ObjectInfo(
          request.bucketName(),
          request.key(),
          p.getBlobSize(),
          p.getLastModified() == null ? null : p.getLastModified().toInstant(),
          p.getETag(),
          null,
          null,
          p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
          Map.of());
    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to head object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public StorageObject getObject(GetObjectRequest request) {
    try {
      BlobClient blob = blob(request.bucketName(), request.key());
      if (!blob.exists()) {
        throw StorageException.notFound(
            PROVIDER_ID, "Object not found: " + request.bucketName() + "/" + request.key(), null);
      }

      BlobProperties p = blob.getProperties();

      InputStream stream;
      if (request.rangeStartInclusive() != null || request.rangeEndInclusive() != null) {
        long start = request.rangeStartInclusive() == null ? 0L : request.rangeStartInclusive();
        Long count =
            request.rangeEndInclusive() == null ? null : (request.rangeEndInclusive() - start + 1);
        BlobRange range = new BlobRange(start, count);
        stream = blob.openInputStream(new BlobInputStreamOptions().setRange(range));
      } else {
        stream = blob.openInputStream();
      }

      DefaultObjectMetadata md =
          new DefaultObjectMetadata(
              p.getContentType(),
              p.getBlobSize(),
              p.getETag(),
              null,
              p.getLastModified() == null ? null : p.getLastModified().toInstant(),
              p.getMetadata() == null ? Map.of() : Map.copyOf(p.getMetadata()),
              Map.of(),
              Map.of(),
              p.getContentEncoding(),
              p.getCacheControl(),
              p.getContentDisposition(),
              p.getContentLanguage());

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

      if (request.bytes() != null) {
        try (InputStream in = new ByteArrayInputStream(request.bytes())) {
          blob.upload(in, request.bytes().length, true); // overwrite=true
        }
      } else if (request.stream() != null) {
        blob.upload(request.stream(), request.contentLength(), true); // overwrite=true
      } else if (request.filePath() != null) {
        blob.uploadFromFile(request.filePath().toString(), true); // overwrite=true
      } else {
        throw new IllegalArgumentException(
            "Exactly one of filePath, bytes, or stream must be provided");
      }

      // Apply headers + metadata AFTER upload (SDK limitation)
      if (headers.getContentType() != null) {
        blob.setHttpHeaders(headers);
      }
      if (!metadata.isEmpty()) {
        blob.setMetadata(metadata);
      }

      BlobProperties p = blob.getProperties();

      return new PutObjectResult(request.bucketName(), request.key(), p.getETag(), null);

    } catch (StorageException e) {
      throw e;
    } catch (BlobStorageException e) {
      throw mapException("Failed to put object: " + request.bucketName() + "/" + request.key(), e);
    } catch (Exception e) {
      throw StorageException.internal(
          PROVIDER_ID, "Failed to put object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public void deleteObject(DeleteObjectRequest request) {
    try {
      blob(request.bucketName(), request.key()).deleteIfExists();
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
