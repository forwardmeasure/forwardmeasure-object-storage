package com.forwardmeasure.objectstorage.gcs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.forwardmeasure.objectstorage.Bucket;
import com.forwardmeasure.objectstorage.DefaultObjectMetadata;
import com.forwardmeasure.objectstorage.ObjectInfo;
import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.StorageObject;
import com.forwardmeasure.objectstorage.core.NamedStorageClientConfig;
import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageOptions;

public final class GcsStorageClient implements StorageClient {
    private static final String PROVIDER_ID = "gcs";

    private final NamedStorageClientConfig config;
    private final Storage storage;
    private final Credentials creds;

    public GcsStorageClient(NamedStorageClientConfig config) {
        this.config = config;
        StorageAndCreds sc = buildStorage(config);
        this.storage = sc.storage();
        this.creds = sc.creds();
    }

    @Override
    public List<Bucket> listBuckets() {
        try {
            Page<com.google.cloud.storage.Bucket> page = storage.list(BucketListOption.pageSize(1000));
            List<Bucket> out = new ArrayList<>();
            for (com.google.cloud.storage.Bucket b : page.iterateAll()) {
                out.add(new Bucket(
                        b.getName(),
                        b.getCreateTimeOffsetDateTime().toInstant(),
                        b.getLocation(),
                        Map.of(),
                        Map.of()));
            }
            return out;
        } catch (Exception e) {
            throw mapException("Failed to list buckets", e);
        }
    }

    @Override
    public Bucket createBucket(CreateBucketRequest request) {
        final String bucket = request.bucketName();
        try {
            BucketInfo.Builder builder = BucketInfo.newBuilder(bucket);

            String location = request.attributes() == null ? null : request.attributes().get("location");
            if (location != null && !location.isBlank()) {
                builder.setLocation(location);
            }

            com.google.cloud.storage.Bucket created = storage.create(builder.build());

            return new Bucket(
                    created.getName(),
                    millisToInstant(created.getCreateTime()),
                    created.getLocation(),
                    request.tags(),
                    request.attributes());

        } catch (com.google.cloud.storage.StorageException e) {
            if (e.getCode() == 409) {
                throw StorageException.conflict(PROVIDER_ID, "Bucket already exists: " + bucket, e);
            }
            throw mapException("Failed to create bucket: " + bucket, e);
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
            storage.delete(bucket);
        } catch (com.google.cloud.storage.StorageException e) {
            if (e.getCode() == 404) {
                return;
            }
            throw mapException("Failed to delete bucket: " + bucket, e);
        } catch (Exception e) {
            throw mapException("Failed to delete bucket: " + bucket, e);
        }
    }

    @Override
    public boolean bucketExists(String bucketName) {
        try {
            return storage.get(bucketName) != null;
        } catch (Exception e) {
            throw mapException("Failed to check bucket existence: " + bucketName, e);
        }
    }

    @Override
    public Optional<Bucket> getBucket(String bucketName) {
        try {
            com.google.cloud.storage.Bucket bucket = storage.get(bucketName);
            if (bucket == null) {
                return Optional.empty();
            }

            return Optional.of(new Bucket(
                    bucket.getName(),
                    millisToInstant(bucket.getCreateTime()),
                    bucket.getLocation(),
                    Map.of(),
                    Map.of()));
        } catch (Exception e) {
            throw mapException("Failed to get bucket: " + bucketName, e);
        }
    }

    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest request) {
        try {
            List<BlobListOption> options = new ArrayList<>();

            if (request.prefix() != null && !request.prefix().isBlank()) {
                options.add(BlobListOption.prefix(request.prefix()));
            }
            if (request.delimiter() != null && !request.delimiter().isBlank()) {
                options.add(BlobListOption.delimiter(request.delimiter()));
            }
            if (request.maxKeys() > 0) {
                options.add(BlobListOption.pageSize(request.maxKeys()));
            }
            if (request.continuationToken() != null && !request.continuationToken().isBlank()) {
                options.add(BlobListOption.pageToken(request.continuationToken()));
            }

            Page<Blob> page = storage.list(request.bucketName(), options.toArray(BlobListOption[]::new));

            List<ObjectInfo> out = new ArrayList<>();
            for (Blob blob : page.getValues()) {
                out.add(new ObjectInfo(
                        request.bucketName(),
                        blob.getName(),
                        blob.getSize() == null ? 0L : blob.getSize(),
                        millisToInstant(blob.getUpdateTime()),
                        blob.getEtag(),
                        blob.getGeneration() == null ? null : String.valueOf(blob.getGeneration()),
                        blob.getStorageClass() == null ? null : blob.getStorageClass().name(),
                        blob.getMetadata() == null ? Map.of() : blob.getMetadata(),
                        Map.of()));
            }

            String next = page.getNextPageToken();
            boolean truncated = next != null && !next.isBlank();

            return new ListObjectsResponse(out, next, truncated);

        } catch (Exception e) {
            throw mapException("Failed to list objects: " + request.bucketName(), e);
        }
    }

    @Override
    public ObjectInfo headObject(HeadObjectRequest request) {
        try {
            Blob blob = storage.get(BlobId.of(request.bucketName(), request.key()));
            if (blob == null) {
                throw StorageException.notFound(
                        PROVIDER_ID,
                        "Object not found: " + request.bucketName() + "/" + request.key(),
                        null);
            }

            return new ObjectInfo(
                    request.bucketName(),
                    request.key(),
                    blob.getSize() == null ? 0L : blob.getSize(),
                    millisToInstant(blob.getUpdateTime()),
                    blob.getEtag(),
                    blob.getGeneration() == null ? null : String.valueOf(blob.getGeneration()),
                    blob.getStorageClass() == null ? null : blob.getStorageClass().name(),
                    blob.getMetadata() == null ? Map.of() : blob.getMetadata(),
                    Map.of());

        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw mapException("Failed to head object: " + request.bucketName() + "/" + request.key(), e);
        }
    }

    @Override
    public StorageObject getObject(GetObjectRequest request) {
        try {
            BlobId id = BlobId.of(request.bucketName(), request.key());
            Blob blob = storage.get(id);
            if (blob == null) {
                throw StorageException.notFound(
                        PROVIDER_ID,
                        "Object not found: " + request.bucketName() + "/" + request.key(),
                        null);
            }

            DefaultObjectMetadata metadata = new DefaultObjectMetadata(
                    blob.getContentType(),
                    blob.getSize() == null ? 0L : blob.getSize(),
                    blob.getEtag(),
                    blob.getGeneration() == null ? null : String.valueOf(blob.getGeneration()),
                    millisToInstant(blob.getUpdateTime()),
                    blob.getMetadata() == null ? Map.of() : blob.getMetadata(),
                    Map.of(),
                    Map.of(),
                    blob.getContentEncoding(),
                    blob.getCacheControl(),
                    blob.getContentDisposition(),
                    blob.getContentLanguage());

            ReadChannel channel = storage.reader(id);

            Long start = request.rangeStartInclusive();
            Long end = request.rangeEndInclusive();
            if (start != null && start > 0) {
                channel.seek(start);
            }

            InputStream base = Channels.newInputStream(channel);
            InputStream content = (end == null)
                    ? base
                    : new BoundedInputStream(base, rangeLimit(start, end));

            return new GcsStorageObject(request.bucketName(), request.key(), metadata, channel, content);

        } catch (StorageException e) {
            throw e;
        } catch (Exception e) {
            throw mapException("Failed to get object: " + request.bucketName() + "/" + request.key(), e);
        }
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
        try {
            BlobId id = BlobId.of(request.bucketName(), request.key());
            BlobInfo.Builder info = BlobInfo.newBuilder(id);

            if (request.contentType() != null && !request.contentType().isBlank()) {
                info.setContentType(request.contentType());
            }
            if (request.userMetadata() != null && !request.userMetadata().isEmpty()) {
                info.setMetadata(request.userMetadata());
            }

            Blob blob;
            if (request.filePath() != null) {
                blob = storage.createFrom(info.build(), request.filePath());
            } else if (request.bytes() != null) {
                blob = storage.create(info.build(), request.bytes());
            } else if (request.stream() != null) {
                blob = writeStream(info.build(), request.stream());
            } else {
                throw new IllegalArgumentException("Exactly one of filePath, bytes, or stream must be provided");
            }

            return new PutObjectResult(
                    request.bucketName(),
                    request.key(),
                    blob.getEtag(),
                    blob.getGeneration() == null ? null : String.valueOf(blob.getGeneration()));

        } catch (Exception e) {
            throw mapException("Failed to put object: " + request.bucketName() + "/" + request.key(), e);
        }
    }

    @Override
    public void deleteObject(DeleteObjectRequest request) {
        try {
            storage.delete(BlobId.of(request.bucketName(), request.key()));
        } catch (Exception e) {
            if (e instanceof com.google.cloud.storage.StorageException g && g.getCode() == 404) {
                return;
            }
            throw mapException("Failed to delete object: " + request.bucketName() + "/" + request.key(), e);
        }
    }

    @Override
    public PresignedRequest presignGet(PresignGetRequest request) {
        return signUrl(request.bucketName(), request.key(), request.expires(), HttpMethod.GET, null);
    }

    @Override
    public PresignedRequest presignPut(PresignPutRequest request) {
        return signUrl(request.bucketName(), request.key(), request.expires(), HttpMethod.PUT, request.contentType());
    }

    @Override
    public Object unwrap() {
        return storage;
    }

    @Override
    public void close() {
        // google-cloud-storage Storage has no close()
    }

    private void deleteAllObjects(String bucket) {
        String pageToken = null;
        while (true) {
            Page<Blob> page = (pageToken == null)
                    ? storage.list(bucket)
                    : storage.list(bucket, BlobListOption.pageToken(pageToken));

            for (Blob blob : page.getValues()) {
                storage.delete(blob.getBlobId());
            }

            pageToken = page.getNextPageToken();
            if (pageToken == null || pageToken.isBlank()) {
                break;
            }
        }
    }

    private Blob writeStream(BlobInfo info, InputStream in) throws IOException {
        try (var channel = storage.writer(info)) {
            in.transferTo(Channels.newOutputStream(channel));
        }
        return storage.get(info.getBlobId());
    }

    private PresignedRequest signUrl(String bucket, String key, Duration expires, HttpMethod method, String contentType) {
        try {
            BlobInfo info = BlobInfo.newBuilder(BlobId.of(bucket, key)).build();

            List<SignUrlOption> options = new ArrayList<>();
            options.add(SignUrlOption.httpMethod(method));
            options.add(SignUrlOption.withV4Signature());

            if (creds instanceof ServiceAccountSigner signer) {
                options.add(SignUrlOption.signWith(signer));
            }

            if (contentType != null && !contentType.isBlank()) {
                options.add(SignUrlOption.withContentType());
                BlobId id = BlobId.of(bucket, key);
                BlobInfo.Builder builder = BlobInfo.newBuilder(id);
                builder.setContentType(contentType);
                info = builder.build();
            }

            URL url = storage.signUrl(
                    info,
                    expires.toSeconds(),
                    TimeUnit.SECONDS,
                    options.toArray(SignUrlOption[]::new));

            URI capability = config.publicEndpoint()
                    .map(endpoint -> publicUri(endpoint, url))
                    .orElseGet(() -> URI.create(url.toString()));
            Map<String, String> headers = contentType == null || contentType.isBlank()
                    ? Map.of()
                    : Map.of("content-type", contentType);
            return new PresignedRequest(capability, headers);

        } catch (IllegalStateException e) {
            throw StorageException.invalidRequest(
                    PROVIDER_ID,
                    "Cannot sign URL with current credentials (need ServiceAccountSigner-capable creds)",
                    e);
        } catch (Exception e) {
            throw mapException("Failed to presign " + method + ": " + bucket + "/" + key, e);
        }
    }

    private static URI publicUri(String endpoint, URL signed) {
        URI base = URI.create(endpoint);
        String basePath = Optional.ofNullable(base.getRawPath()).orElse("");
        String signedPath = signed.getPath();
        String path = (basePath.endsWith("/")
                ? basePath.substring(0, basePath.length() - 1)
                : basePath) + signedPath;
        try {
            return new URI(
                    base.getScheme(),
                    base.getRawAuthority(),
                    path,
                    signed.getQuery(),
                    null);
        } catch (java.net.URISyntaxException failure) {
            throw new IllegalArgumentException("Invalid public GCS endpoint", failure);
        }
    }

    private StorageException mapException(String message, Exception e) {
        if (e instanceof StorageException se) {
            return se;
        }
        if (e instanceof com.google.cloud.storage.StorageException g) {
            int code = g.getCode();
            if (code == 404) {
                return StorageException.notFound(PROVIDER_ID, message, g);
            }
            if (code == 403) {
                return StorageException.accessDenied(PROVIDER_ID, message, g);
            }
            if (code == 409) {
                return StorageException.conflict(PROVIDER_ID, message, g);
            }
            if (code == 429 || code == 500 || code == 502 || code == 503 || code == 504) {
                return StorageException.retryable(PROVIDER_ID, message, g);
            }
        }
        return StorageException.internal(PROVIDER_ID, message, e);
    }

    private static Instant millisToInstant(Long millis) {
        return millis == null ? null : Instant.ofEpochMilli(millis);
    }

    private static long rangeLimit(Long start, Long end) {
        long s = start == null ? 0L : start;
        long e = end;
        if (e < s) {
            return 0L;
        }
        return (e - s) + 1;
    }

    private static StorageAndCreds buildStorage(NamedStorageClientConfig cfg) {
        try {
            String projectId = cfg.properties().get("gcp.projectId");
            String credsPath = cfg.properties().get("gcp.credentialsPath");

            StorageOptions.Builder builder = StorageOptions.newBuilder();

            Credentials creds;
            if (credsPath != null && !credsPath.isBlank()) {
                try (InputStream in = Files.newInputStream(Path.of(credsPath))) {
                    creds = GoogleCredentials.fromStream(in);
                }
                // Emulators do not expose an OAuth token endpoint. Keep the
                // service-account credential as the URL signer while using
                // anonymous transport credentials for an explicitly configured
                // endpoint. Native GCS continues to authenticate with the same
                // service-account credential.
                builder.setCredentials(cfg.endpoint().isPresent()
                        ? NoCredentials.getInstance()
                        : creds);
            } else if (cfg.endpoint().isPresent()) {
                creds = NoCredentials.getInstance();
                builder.setCredentials(creds);
            } else {
                creds = GoogleCredentials.getApplicationDefault();
                builder.setCredentials(creds);
            }

            if (projectId != null && !projectId.isBlank()) {
                builder.setProjectId(projectId);
            }

            cfg.endpoint().ifPresent(builder::setHost);

            return new StorageAndCreds(builder.build().getService(), creds);

        } catch (Exception e) {
            throw StorageException.invalidRequest(PROVIDER_ID, "Failed to initialize GCS client", e);
        }
    }

    private static final class BoundedInputStream extends InputStream {
        private final InputStream delegate;
        private long remaining;

        private BoundedInputStream(InputStream delegate, long maxBytes) {
            this.delegate = delegate;
            this.remaining = maxBytes;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int b = delegate.read();
            if (b >= 0) {
                remaining--;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int toRead = (int) Math.min(len, remaining);
            int n = delegate.read(b, off, toRead);
            if (n > 0) {
                remaining -= n;
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class GcsStorageObject implements StorageObject {
        private final String bucket;
        private final String key;
        private final DefaultObjectMetadata metadata;
        private final ReadChannel channel;
        private final InputStream stream;

        private GcsStorageObject(
                String bucket,
                String key,
                DefaultObjectMetadata metadata,
                ReadChannel channel,
                InputStream stream) {
            this.bucket = bucket;
            this.key = key;
            this.metadata = metadata;
            this.channel = channel;
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
                channel.close();
            } catch (Exception ignored) {
            }
            try {
                stream.close();
            } catch (Exception ignored) {
            }
        }
    }

    private record StorageAndCreds(Storage storage, Credentials creds) {
    }
}
