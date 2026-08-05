package com.forwardmeasure.objectstorage;

import java.io.InputStream;

/**
 * Represents an object stored in a bucket.
 *
 * Callers MUST close either:
 * - the returned content() stream, or
 * - this StorageObject (recommended), which should close any underlying
 * resources.
 */
public interface StorageObject extends AutoCloseable {

    /**
     * @return The bucket/container name containing this object
     */
    String bucketName();

    /**
     * @return The object key (path)
     */
    String key();

    /**
     * @return Metadata associated with this object
     */
    ObjectMetadata metadata();

    /**
     * @return InputStream for object content. Closing the stream should release
     *         underlying resources.
     */
    InputStream content();

    /**
     * Close any underlying resources (HTTP response body, connections, etc.).
     * Implementations should be idempotent.
     */
    @Override
    void close();
}
