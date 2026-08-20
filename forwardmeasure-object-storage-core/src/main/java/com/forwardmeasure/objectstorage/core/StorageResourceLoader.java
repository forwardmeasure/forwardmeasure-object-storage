package com.forwardmeasure.objectstorage.core;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import com.forwardmeasure.objectstorage.StorageObject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Loads small configuration and specification resources from a URI.
 *
 * <p>Supported locations are:
 *
 * <ul>
 *   <li>{@code classpath:/path/to/resource}
 *   <li>{@code file:///absolute/path/to/resource}
 *   <li>storage-provider URIs such as {@code gs://}, {@code s3://} and {@code abfs://}, resolved by
 *       {@link StorageClientRegistry#getForUri}
 * </ul>
 *
 * <p>The remote schemes are deliberately not hard-coded. Applications map schemes and URI prefixes
 * to named clients in {@link StorageConfigGroup}, so the same loader works with every configured
 * storage provider.
 */
public final class StorageResourceLoader {

  private final StorageClientRegistry storageClientRegistry;
  private final ClassLoader fallbackClassLoader;

  public StorageResourceLoader(StorageClientRegistry storageClientRegistry) {
    this(storageClientRegistry, StorageResourceLoader.class.getClassLoader());
  }

  StorageResourceLoader(
      StorageClientRegistry storageClientRegistry, ClassLoader fallbackClassLoader) {
    this.storageClientRegistry = storageClientRegistry;
    this.fallbackClassLoader = fallbackClassLoader;
  }

  public byte[] readBytes(String location) throws IOException {
    URI uri = parse(location);
    String scheme = uri.getScheme();

    if ("classpath".equalsIgnoreCase(scheme)) {
      return readClasspath(uri, location);
    }
    if ("file".equalsIgnoreCase(scheme)) {
      return Files.readAllBytes(Path.of(uri));
    }

    return readStorage(uri, location);
  }

  public String readString(String location) throws IOException {
    return readString(location, StandardCharsets.UTF_8);
  }

  public String readString(String location, Charset charset) throws IOException {
    if (charset == null) {
      throw new IllegalArgumentException("charset is required");
    }
    return new String(readBytes(location), charset);
  }

  /**
   * Returns an independently owned in-memory stream. Remote storage objects are fully read and
   * closed before this method returns.
   */
  public InputStream openStream(String location) throws IOException {
    return new ByteArrayInputStream(readBytes(location));
  }

  private byte[] readClasspath(URI uri, String location) throws IOException {
    String resourcePath = classpathResourcePath(uri, location);
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    InputStream input =
        contextClassLoader == null ? null : contextClassLoader.getResourceAsStream(resourcePath);
    if (input == null && fallbackClassLoader != null) {
      input = fallbackClassLoader.getResourceAsStream(resourcePath);
    }
    if (input == null) {
      throw new IOException("Classpath resource not found: " + location);
    }

    try (InputStream closeable = input) {
      return closeable.readAllBytes();
    }
  }

  private byte[] readStorage(URI uri, String location) throws IOException {
    if (storageClientRegistry == null) {
      throw new IOException("No StorageClientRegistry is available for: " + location);
    }

    String bucket = uri.getHost();
    String key = stripLeadingSlash(uri.getPath());
    if (bucket == null || bucket.isBlank()) {
      throw new IOException("Storage URI has no bucket/container/host: " + location);
    }
    if (key.isBlank()) {
      throw new IOException("Storage URI has no object key/path: " + location);
    }

    try {
      StorageClient client = storageClientRegistry.getForUri(uri);
      try (StorageObject object =
          client.getObject(StorageClient.GetObjectRequest.of(bucket, key))) {
        return object.content().readAllBytes();
      }
    } catch (StorageException e) {
      throw new IOException(
          "Storage resource could not be read from " + location + ": " + e.getMessage(), e);
    }
  }

  private URI parse(String location) throws IOException {
    if (location == null || location.isBlank()) {
      throw new IOException("Resource URI is required");
    }

    try {
      URI uri = URI.create(location.trim());
      if (uri.getScheme() == null || uri.getScheme().isBlank()) {
        throw new IOException("Resource URI has no scheme: " + location);
      }
      return uri;
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid resource URI: " + location, e);
    }
  }

  private String classpathResourcePath(URI uri, String location) throws IOException {
    String path = uri.getPath();
    if (path == null || path.isBlank()) {
      path = uri.getSchemeSpecificPart();
    }
    path = stripLeadingSlash(path);
    if (path.isBlank()) {
      throw new IOException("Classpath URI does not identify a resource: " + location);
    }
    return path;
  }

  private String stripLeadingSlash(String value) {
    if (value == null) {
      return "";
    }
    return value.startsWith("/") ? value.substring(1) : value;
  }
}
