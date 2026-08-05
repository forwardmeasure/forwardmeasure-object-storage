package com.forwardmeasure.objectstorage;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Provider-agnostic object summary (suitable for list/head operations).
 * Does not include content.
 *
 * Notes:
 * - contentTag is an opaque validation/identity token (e.g., S3 ETag, Azure
 * ETag).
 * - versionId is a provider version/generation identifier when supported.
 */
public record ObjectInfo(
        String bucketName,
        String key,
        long size,
        Instant lastModified,
        String contentTag,
        String versionId,
        String storageClass,
        Map<String, String> userMetadata,
        Map<String, String> attributes) {
    public ObjectInfo {
        if (bucketName == null || bucketName.isBlank()) {
            throw new IllegalArgumentException("bucketName is required");
        }
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key is required");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
        userMetadata = (userMetadata == null) ? Map.of() : Map.copyOf(userMetadata);
        attributes = (attributes == null) ? Map.of() : Map.copyOf(attributes);
    }

    public Optional<Instant> lastModifiedOptional() {
        return Optional.ofNullable(lastModified);
    }

    public Optional<String> contentTagOptional() {
        return Optional.ofNullable(contentTag);
    }

    public Optional<String> versionIdOptional() {
        return Optional.ofNullable(versionId);
    }

    public Optional<String> storageClassOptional() {
        return Optional.ofNullable(storageClass);
    }
}
