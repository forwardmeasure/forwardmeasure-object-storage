package com.forwardmeasure.objectstorage;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main client interface for interacting with storage providers.
 *
 * Idioms:
 * - Uses normal return values; failures throw StorageException (runtime).
 * - Stateless w.r.t. bucket selection: every operation takes an explicit
 * bucket.
 * - Provider-agnostic request/response records for extensibility.
 */
public interface StorageClient extends AutoCloseable {
    static Logger logger = LoggerFactory.getLogger(StorageClient.class);

    // =========================
    // Buckets
    // =========================

    List<Bucket> listBuckets() throws StorageException;

    Bucket createBucket(CreateBucketRequest request) throws StorageException;

    void deleteBucket(DeleteBucketRequest request) throws StorageException;

    boolean bucketExists(String bucketName) throws StorageException;

    Optional<Bucket> getBucket(String bucketName) throws StorageException;

    // =========================
    // Objects
    // =========================

    ListObjectsResponse listObjects(ListObjectsRequest request) throws StorageException;

    ObjectInfo headObject(HeadObjectRequest request) throws StorageException;

    StorageObject getObject(GetObjectRequest request) throws StorageException;

    PutObjectResult putObject(PutObjectRequest request) throws StorageException;

    void deleteObject(DeleteObjectRequest request) throws StorageException;

    // =========================
    // URLs (optional capability per provider)
    // =========================

    PresignedRequest presignGet(PresignGetRequest request) throws StorageException;

    PresignedRequest presignPut(PresignPutRequest request) throws StorageException;

    /**
     * Provider-specific underlying client (e.g., AWS SDK S3Client, GCS Storage,
     * Azure BlobServiceClient).
     */
    Object unwrap();

    @Override
    void close() throws StorageException;

    // =====================================================================================
    // Requests / Responses (nested for now; we can extract later if you want)
    // =====================================================================================

    record CreateBucketRequest(
            String bucketName,
            Map<String, String> tags,
            Map<String, String> attributes) {
        public CreateBucketRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            tags = tags == null ? Map.of() : Map.copyOf(tags);
            attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        }
    }

    record DeleteBucketRequest(
            String bucketName,
            boolean forceDeleteContents) {
        public DeleteBucketRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
        }
    }

    record ListObjectsRequest(
            String bucketName,
            String prefix,
            String delimiter,
            int maxKeys,
            String continuationToken) {
        public ListObjectsRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (maxKeys < 0) {
                throw new IllegalArgumentException("maxKeys must be >= 0");
            }
        }

        public static ListObjectsRequest of(String bucketName) {
            return new ListObjectsRequest(bucketName, null, null, 0, null);
        }
    }

    record ListObjectsResponse(
            List<ObjectInfo> objects,
            String nextContinuationToken,
            boolean isTruncated) {
        public ListObjectsResponse {
            objects = objects == null ? List.of() : List.copyOf(objects);
        }
    }

    record HeadObjectRequest(String bucketName, String key) {
        public HeadObjectRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
        }
    }

    record GetObjectRequest(
            String bucketName,
            String key,
            Long rangeStartInclusive,
            Long rangeEndInclusive) {
        public GetObjectRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
            if (rangeStartInclusive != null && rangeStartInclusive < 0) {
                throw new IllegalArgumentException("rangeStartInclusive must be >= 0");
            }
            if (rangeEndInclusive != null && rangeEndInclusive < 0) {
                throw new IllegalArgumentException("rangeEndInclusive must be >= 0");
            }
            if (rangeStartInclusive != null && rangeEndInclusive != null && rangeEndInclusive < rangeStartInclusive) {
                throw new IllegalArgumentException("rangeEndInclusive must be >= rangeStartInclusive");
            }
        }

        public static GetObjectRequest of(String bucketName, String key) {
            return new GetObjectRequest(bucketName, key, null, null);
        }
    }

    record PutObjectRequest(
            String bucketName,
            String key,
            Path filePath,
            byte[] bytes,
            InputStream stream,
            Long contentLength,
            String contentType,
            Map<String, String> userMetadata) {
        public PutObjectRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
            userMetadata = userMetadata == null ? Map.of() : Map.copyOf(userMetadata);

            int sources = 0;
            if (filePath != null)
                sources++;
            if (bytes != null)
                sources++;
            if (stream != null)
                sources++;

            if (sources != 1) {
                throw new IllegalArgumentException("Exactly one of filePath, bytes, or stream must be provided");
            }
            if (stream != null && (contentLength == null || contentLength < 0)) {
                throw new IllegalArgumentException("contentLength is required and must be >= 0 when stream is used");
            }
        }

        public static PutObjectRequest fromPath(String bucket, String key, Path path, String contentType,
                Map<String, String> userMetadata) {
            return new PutObjectRequest(bucket, key, path, null, null, null, contentType, userMetadata);
        }

        public static PutObjectRequest fromBytes(String bucket, String key, byte[] bytes, String contentType,
                Map<String, String> userMetadata) {
            return new PutObjectRequest(bucket, key, null, bytes, null, null, contentType, userMetadata);
        }

        public static PutObjectRequest fromStream(String bucket, String key, InputStream stream, long contentLength,
                String contentType, Map<String, String> userMetadata) {
            return new PutObjectRequest(bucket, key, null, null, stream, contentLength, contentType, userMetadata);
        }
    }

    record PutObjectResult(
            String bucketName,
            String key,
            String etag,
            String versionId) {
    }

    record DeleteObjectRequest(String bucketName, String key) {
        public DeleteObjectRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
        }
    }

    record PresignGetRequest(
            String bucketName,
            String key,
            Duration expires) {
        public PresignGetRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
            if (expires == null || expires.isNegative() || expires.isZero()) {
                throw new IllegalArgumentException("expires must be > 0");
            }
        }
    }

    record PresignPutRequest(
            String bucketName,
            String key,
            Duration expires,
            String contentType) {
        public PresignPutRequest {
            if (bucketName == null || bucketName.isBlank()) {
                throw new IllegalArgumentException("bucketName is required");
            }
            if (key == null || key.isBlank()) {
                throw new IllegalArgumentException("key is required");
            }
            if (expires == null || expires.isNegative() || expires.isZero()) {
                throw new IllegalArgumentException("expires must be > 0");
            }
        }
    }

    /** A delegated storage capability and the exact headers its caller must send. */
    record PresignedRequest(URI uri, Map<String, String> requiredHeaders) {
        public PresignedRequest {
            if (uri == null || !uri.isAbsolute()) {
                throw new IllegalArgumentException("uri must be absolute");
            }
            requiredHeaders = requiredHeaders == null
                    ? Map.of()
                    : Map.copyOf(requiredHeaders);
        }
    }

    /**
     * Builds a platform URI for a stored object from a backend type string.
     * gcs → gs://
     * s3 / s3-compat → s3://
     * azure / azure-blob → az://
     */
    static String buildPlatformUri(String backend, String bucket, String key) {
        String scheme = switch (backend == null ? "" : backend.trim().toLowerCase()) {
            case "s3", "s3-compat" -> "s3";
            case "gcs" -> "gs";
            case "azure", "azure-blob" -> "az";
            default -> throw new IllegalArgumentException(
                    "Unsupported object-storage backend: " + backend);
        };
        return scheme + "://" + bucket + "/" + key;
    }

    /**
     * Buffers an unknown-length InputStream to a temp file, invokes the
     * provided upload action with the buffered file size, then deletes the
     * temp file. The action receives the temp file path and its size in bytes.
     */
    static <T> T bufferAndUpload(
            InputStream stream,
            String tempPrefix,
            String tempSuffix,
            java.util.function.BiFunction<Path, Long, T> uploadAction)
            throws java.io.IOException {
        Path tempFile = Files.createTempFile(tempPrefix, tempSuffix);
        try {
            long size;
            try (InputStream in = stream) {
                size = Files.copy(in, tempFile, StandardCopyOption.REPLACE_EXISTING);
            }
            return uploadAction.apply(tempFile, size);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    /**
     * 
     * @param client
     * @param platformUri
     * @param logLabel
     */
    static void deleteBestEffort(StorageClient client, String platformUri, String logLabel) {
        if (platformUri == null || platformUri.isBlank()) {
            logger.debug("{} has no platformStorageUri — skipping storage delete", logLabel);
            return;
        }

        try {
            java.net.URI uri = java.net.URI.create(platformUri);
            String bucket = uri.getHost();
            String key = uri.getPath() != null && uri.getPath().startsWith("/")
                    ? uri.getPath().substring(1)
                    : uri.getPath();
            if (bucket == null || bucket.isBlank() || key == null || key.isBlank()) {
                logger.warn("{} has unparseable platformStorageUri={} — skipping", logLabel, platformUri);
                return;
            }
            client.deleteObject(new DeleteObjectRequest(bucket, key));
            logger.info("Deleted storage object for {} uri={}", logLabel, platformUri);
        } catch (StorageException e) {
            logger.warn("Storage delete failed for {} uri={} — DB deletion will proceed: {}",
                    logLabel, platformUri, e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error deleting storage object for {} uri={} — DB deletion will proceed",
                    logLabel, platformUri, e);
        }
    }
}
