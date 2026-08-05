package com.forwardmeasure.objectstorage;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * Metadata associated with a storage object.
 */
public interface ObjectMetadata {

    /**
     * @return Content type (MIME type) of the object
     */
    Optional<String> contentType();

    /**
     * @return Content length in bytes
     */
    long contentLength();

    /**
     * @return ETag of the object
     */
    Optional<String> contentTag();

    /**
     * Provider version/generation identifier when supported.
     */
    Optional<String> versionId();

    /**
     * @return Last modified timestamp
     */
    Optional<Instant> lastModified();

    /**
     * @return User-defined metadata key-value pairs
     */
    Map<String, String> userMetadata();

    /**
     * @return System metadata key-value pairs (provider-specific)
     */
    Map<String, String> systemMetadata();

    /**
     * Provider-specific metadata/fields that don’t cleanly map to standard headers.
     */
    Map<String, String> attributes();

    /**
     * @return Content encoding (e.g., gzip)
     */
    Optional<String> contentEncoding();

    /**
     * @return Cache control directives
     */
    Optional<String> cacheControl();

    /**
     * @return Content disposition
     */
    Optional<String> contentDisposition();

    Optional<String> contentLanguage();
}
