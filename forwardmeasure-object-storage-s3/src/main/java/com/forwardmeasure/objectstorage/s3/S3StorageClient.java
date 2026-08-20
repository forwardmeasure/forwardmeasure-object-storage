package com.forwardmeasure.objectstorage.s3;

import com.forwardmeasure.objectstorage.Bucket;
import com.forwardmeasure.objectstorage.DefaultObjectMetadata;
import com.forwardmeasure.objectstorage.ObjectInfo;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.StorageObject;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.BucketLocationConstraint;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

public final class S3StorageClient implements StorageClient {

  private static final String PROVIDER = "s3";

  private final NamedStorageClientConfig config;
  private final S3Client s3;
  private final S3Presigner presigner;

  public S3StorageClient(NamedStorageClientConfig config) {
    this.config = config;
    this.s3 = buildS3Client(config);
    this.presigner = buildPresigner(config);
  }

  // =========================
  // Buckets
  // =========================

  @Override
  public List<Bucket> listBuckets() {
    try {
      ListBucketsResponse resp = s3.listBuckets();
      List<Bucket> out = new ArrayList<>(resp.buckets().size());
      resp.buckets()
          .forEach(b -> out.add(new Bucket(b.name(), b.creationDate(), null, Map.of(), Map.of())));
      return out;
    } catch (Exception e) {
      throw mapException("Failed to list buckets", e);
    }
  }

  @Override
  public Bucket createBucket(CreateBucketRequest request) {
    final String bucket = request.bucketName();
    try {
      software.amazon.awssdk.services.s3.model.CreateBucketRequest.Builder b =
          software.amazon.awssdk.services.s3.model.CreateBucketRequest.builder().bucket(bucket);

      String region = config.region().orElse("us-east-1");
      if (!"us-east-1".equalsIgnoreCase(region)) {
        try {
          b.createBucketConfiguration(
              CreateBucketConfiguration.builder()
                  .locationConstraint(BucketLocationConstraint.fromValue(region))
                  .build());
        } catch (IllegalArgumentException ignored) {
          // S3-compatible services or unknown region strings may not map cleanly.
        }
      }

      s3.createBucket(b.build());

      return new Bucket(bucket, null, null, request.tags(), request.attributes());

    } catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException e) {
      throw StorageException.conflict(PROVIDER, "Bucket already exists: " + bucket, e);
    } catch (Exception e) {
      throw mapException("Failed to create bucket: " + bucket, e);
    }
  }

  @Override
  public void deleteBucket(DeleteBucketRequest request) {
    final String bucket = request.bucketName();
    try {
      if (request.forceDeleteContents()) {
        deleteAllObjects(bucket);
      }

      s3.deleteBucket(
          software.amazon.awssdk.services.s3.model.DeleteBucketRequest.builder()
              .bucket(bucket)
              .build());

    } catch (NoSuchBucketException e) {
      // idempotent no-op
    } catch (Exception e) {
      throw mapException("Failed to delete bucket: " + bucket, e);
    }
  }

  @Override
  public boolean bucketExists(String bucketName) {
    try {
      s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return true;
    } catch (NoSuchBucketException e) {
      return false;
    } catch (Exception e) {
      throw mapException("Failed to check bucket existence: " + bucketName, e);
    }
  }

  @Override
  public Optional<Bucket> getBucket(String bucketName) {
    try {
      s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return Optional.of(new Bucket(bucketName, null, null, Map.of(), Map.of()));
    } catch (NoSuchBucketException e) {
      return Optional.empty();
    } catch (Exception e) {
      throw mapException("Failed to get bucket: " + bucketName, e);
    }
  }

  // =========================
  // Objects
  // =========================

  @Override
  public ListObjectsResponse listObjects(ListObjectsRequest request) {
    try {
      ListObjectsV2Request.Builder b =
          ListObjectsV2Request.builder()
              .bucket(request.bucketName())
              .prefix(request.prefix())
              .delimiter(request.delimiter());

      if (request.maxKeys() > 0) {
        b.maxKeys(request.maxKeys());
      }
      if (request.continuationToken() != null && !request.continuationToken().isBlank()) {
        b.continuationToken(request.continuationToken());
      }

      ListObjectsV2Response resp = s3.listObjectsV2(b.build());

      List<ObjectInfo> objects = new ArrayList<>(resp.contents().size());
      resp.contents()
          .forEach(
              o ->
                  objects.add(
                      new ObjectInfo(
                          request.bucketName(),
                          o.key(),
                          o.size() == null ? 0L : o.size(),
                          o.lastModified(),
                          o.eTag(),
                          null,
                          o.storageClassAsString(),
                          Map.of(),
                          Map.of())));

      return new ListObjectsResponse(
          objects, resp.nextContinuationToken(), Boolean.TRUE.equals(resp.isTruncated()));

    } catch (Exception e) {
      throw mapException("Failed to list objects: " + request.bucketName(), e);
    }
  }

  @Override
  public ObjectInfo headObject(HeadObjectRequest request) {
    try {
      HeadObjectResponse r =
          s3.headObject(
              software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                  .bucket(request.bucketName())
                  .key(request.key())
                  .build());

      return new ObjectInfo(
          request.bucketName(),
          request.key(),
          r.contentLength() == null ? 0L : r.contentLength(),
          r.lastModified(),
          r.eTag(),
          r.versionId(),
          r.storageClassAsString(),
          r.metadata() == null ? Map.of() : r.metadata(),
          Map.of());

    } catch (NoSuchKeyException e) {
      throw StorageException.notFound(
          PROVIDER, "Object not found: " + request.bucketName() + "/" + request.key(), e);
    } catch (NoSuchBucketException e) {
      throw StorageException.notFound(PROVIDER, "Bucket not found: " + request.bucketName(), e);
    } catch (Exception e) {
      throw mapException("Failed to head object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public StorageObject getObject(GetObjectRequest request) {
    try {
      software.amazon.awssdk.services.s3.model.GetObjectRequest.Builder b =
          software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
              .bucket(request.bucketName())
              .key(request.key());

      if (request.rangeStartInclusive() != null || request.rangeEndInclusive() != null) {
        long start = request.rangeStartInclusive() == null ? 0L : request.rangeStartInclusive();
        Long end = request.rangeEndInclusive();
        String range = (end == null) ? ("bytes=" + start + "-") : ("bytes=" + start + "-" + end);
        b.range(range);
      }

      ResponseInputStream<GetObjectResponse> body = s3.getObject(b.build());
      GetObjectResponse r = body.response();

      DefaultObjectMetadata md =
          new DefaultObjectMetadata(
              r.contentType(),
              r.contentLength() == null ? 0L : r.contentLength(),
              r.eTag(),
              r.versionId(),
              r.lastModified(),
              r.metadata() == null ? Map.of() : r.metadata(),
              Map.of(),
              Map.of(),
              r.contentEncoding(),
              r.cacheControl(),
              r.contentDisposition(),
              r.contentLanguage());

      return new S3StorageObject(request.bucketName(), request.key(), md, body);

    } catch (NoSuchKeyException e) {
      throw StorageException.notFound(
          PROVIDER, "Object not found: " + request.bucketName() + "/" + request.key(), e);
    } catch (NoSuchBucketException e) {
      throw StorageException.notFound(PROVIDER, "Bucket not found: " + request.bucketName(), e);
    } catch (Exception e) {
      throw mapException("Failed to get object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public PutObjectResult putObject(PutObjectRequest request) {
    try {
      software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder b =
          software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
              .bucket(request.bucketName())
              .key(request.key())
              .contentType(request.contentType())
              .metadata(request.userMetadata());

      PutObjectResponse resp;

      if (request.filePath() != null) {
        resp = s3.putObject(b.build(), request.filePath());
      } else if (request.bytes() != null) {
        resp = s3.putObject(b.build(), RequestBody.fromBytes(request.bytes()));
      } else if (request.stream() != null) {
        resp =
            s3.putObject(
                b.build(), RequestBody.fromInputStream(request.stream(), request.contentLength()));
      } else {
        throw new IllegalArgumentException(
            "Exactly one of filePath, bytes, or stream must be provided");
      }

      return new PutObjectResult(
          request.bucketName(), request.key(), resp.eTag(), resp.versionId());

    } catch (NoSuchBucketException e) {
      throw StorageException.notFound(PROVIDER, "Bucket not found: " + request.bucketName(), e);
    } catch (Exception e) {
      throw mapException("Failed to put object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public void deleteObject(DeleteObjectRequest request) {
    try {
      s3.deleteObject(
          software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
              .bucket(request.bucketName())
              .key(request.key())
              .build());
    } catch (NoSuchBucketException | NoSuchKeyException e) {
      // idempotent
    } catch (Exception e) {
      throw mapException(
          "Failed to delete object: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  // =========================
  // Presign
  // =========================

  @Override
  public PresignedRequest presignGet(PresignGetRequest request) {
    try {
      software.amazon.awssdk.services.s3.model.GetObjectRequest get =
          software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
              .bucket(request.bucketName())
              .key(request.key())
              .build();

      GetObjectPresignRequest pr =
          GetObjectPresignRequest.builder()
              .signatureDuration(request.expires())
              .getObjectRequest(get)
              .build();

      PresignedGetObjectRequest out = presigner.presignGetObject(pr);
      return new PresignedRequest(out.url().toURI(), requiredHeaders(out.signedHeaders()));

    } catch (Exception e) {
      throw mapException("Failed to presign GET: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public PresignedRequest presignPut(PresignPutRequest request) {
    try {
      software.amazon.awssdk.services.s3.model.PutObjectRequest.Builder put =
          software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
              .bucket(request.bucketName())
              .key(request.key());

      if (request.contentType() != null && !request.contentType().isBlank()) {
        put.contentType(request.contentType());
      }

      PutObjectPresignRequest pr =
          PutObjectPresignRequest.builder()
              .signatureDuration(request.expires())
              .putObjectRequest(put.build())
              .build();

      PresignedPutObjectRequest out = presigner.presignPutObject(pr);
      return new PresignedRequest(out.url().toURI(), requiredHeaders(out.signedHeaders()));

    } catch (Exception e) {
      throw mapException("Failed to presign PUT: " + request.bucketName() + "/" + request.key(), e);
    }
  }

  @Override
  public Object unwrap() {
    return s3;
  }

  @Override
  public void close() {
    try {
      presigner.close();
    } catch (Exception ignored) {
    }
    try {
      s3.close();
    } catch (Exception ignored) {
    }
  }

  // =========================
  // Internals
  // =========================

  private void deleteAllObjects(String bucket) {
    String token = null;

    while (true) {
      ListObjectsV2Response list =
          s3.listObjectsV2(
              ListObjectsV2Request.builder().bucket(bucket).continuationToken(token).build());

      if (!list.contents().isEmpty()) {
        List<ObjectIdentifier> ids =
            list.contents().stream()
                .map(o -> ObjectIdentifier.builder().key(o.key()).build())
                .toList();

        for (int i = 0; i < ids.size(); i += 1000) {
          int end = Math.min(i + 1000, ids.size());
          List<ObjectIdentifier> chunk = ids.subList(i, end);

          s3.deleteObjects(
              DeleteObjectsRequest.builder()
                  .bucket(bucket)
                  .delete(Delete.builder().objects(chunk).build())
                  .build());
        }
      }

      if (!Boolean.TRUE.equals(list.isTruncated())) {
        break;
      }
      token = list.nextContinuationToken();
    }
  }

  private S3Client buildS3Client(NamedStorageClientConfig cfg) {
    S3ClientBuilder b = S3Client.builder();

    if (cfg.accessKey().isPresent() && cfg.secretKey().isPresent()) {
      AwsBasicCredentials creds =
          AwsBasicCredentials.create(cfg.accessKey().get(), cfg.secretKey().get());
      b.credentialsProvider(StaticCredentialsProvider.create(creds));
    } else {
      b.credentialsProvider(DefaultCredentialsProvider.builder().build());
    }

    b.region(Region.of(cfg.region().orElse("us-east-1")));
    cfg.publicEndpoint().or(cfg::endpoint).ifPresent(ep -> b.endpointOverride(URI.create(ep)));

    b.serviceConfiguration(
        S3Configuration.builder()
            .pathStyleAccessEnabled(cfg.pathStyleAccess().orElse(false))
            .build());

    return b.build();
  }

  private static Map<String, String> requiredHeaders(Map<String, List<String>> signedHeaders) {
    Map<String, String> required = new java.util.LinkedHashMap<>();
    signedHeaders.forEach(
        (name, values) -> {
          if (!name.equalsIgnoreCase("host")
              && !name.equalsIgnoreCase("content-length")
              && !values.isEmpty()) {
            required.put(name, String.join(",", values));
          }
        });
    return required;
  }

  private S3Presigner buildPresigner(NamedStorageClientConfig cfg) {
    S3Presigner.Builder b = S3Presigner.builder();

    if (cfg.accessKey().isPresent() && cfg.secretKey().isPresent()) {
      AwsBasicCredentials creds =
          AwsBasicCredentials.create(cfg.accessKey().get(), cfg.secretKey().get());
      b.credentialsProvider(StaticCredentialsProvider.create(creds));
    } else {
      b.credentialsProvider(DefaultCredentialsProvider.builder().build());
    }

    b.region(Region.of(cfg.region().orElse("us-east-1")));
    cfg.endpoint().ifPresent(ep -> b.endpointOverride(URI.create(ep)));
    b.serviceConfiguration(
        S3Configuration.builder()
            .pathStyleAccessEnabled(cfg.pathStyleAccess().orElse(false))
            .build());

    return b.build();
  }

  private StorageException mapException(String msg, Exception e) {
    if (e instanceof NoSuchBucketException || e instanceof NoSuchKeyException) {
      return StorageException.notFound(PROVIDER, msg, e);
    }
    if (e instanceof S3Exception s3e) {
      int code = s3e.statusCode();
      if (code == 404) {
        return StorageException.notFound(PROVIDER, msg, s3e);
      }
      if (code == 403) {
        return StorageException.accessDenied(PROVIDER, msg, s3e);
      }
      if (code == 409) {
        return StorageException.conflict(PROVIDER, msg, s3e);
      }
      if (code == 429 || code == 500 || code == 502 || code == 503 || code == 504) {
        return StorageException.retryable(PROVIDER, msg, s3e);
      }
    }
    return StorageException.internal(PROVIDER, msg, e);
  }

  private static final class S3StorageObject implements StorageObject {
    private final String bucket;
    private final String key;
    private final DefaultObjectMetadata metadata;
    private final ResponseInputStream<GetObjectResponse> body;

    private S3StorageObject(
        String bucket,
        String key,
        DefaultObjectMetadata metadata,
        ResponseInputStream<GetObjectResponse> body) {
      this.bucket = bucket;
      this.key = key;
      this.metadata = metadata;
      this.body = body;
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
      return body;
    }

    @Override
    public void close() {
      try {
        body.close();
      } catch (Exception ignored) {
      }
    }
  }
}
