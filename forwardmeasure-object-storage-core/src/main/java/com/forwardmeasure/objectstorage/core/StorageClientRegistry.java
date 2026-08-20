package com.forwardmeasure.objectstorage.core;

import com.forwardmeasure.objectstorage.StorageClient;
import com.forwardmeasure.objectstorage.StorageException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StorageClientRegistry implements AutoCloseable {

  private final StorageConfigGroup config;
  private final Map<String, StorageClientProvider> providers;
  private final ConcurrentMap<String, StorageClient> cache = new ConcurrentHashMap<>();

  public StorageClientRegistry(StorageConfigGroup config) {
    this(config, ServiceLoader.load(StorageClientProvider.class));
  }

  public StorageClientRegistry(
      StorageConfigGroup config, Iterable<StorageClientProvider> providers) {
    this.config = Objects.requireNonNull(config, "config");
    Objects.requireNonNull(providers, "providers");
    Map<String, StorageClientProvider> discovered = new LinkedHashMap<>();
    for (StorageClientProvider provider : providers) {
      String backend = trimToNull(Objects.requireNonNull(provider, "provider").backend());
      if (backend == null) {
        throw new IllegalArgumentException("StorageClientProvider backend is required");
      }
      StorageClientProvider duplicate = discovered.putIfAbsent(backend, provider);
      if (duplicate != null) {
        throw new IllegalArgumentException(
            "Multiple StorageClientProvider implementations found for backend: " + backend);
      }
    }
    this.providers = Map.copyOf(discovered);
  }

  public StorageClient get(String clientName) {
    String key = trimToNull(clientName);

    if (key == null) {
      throw StorageException.invalidRequest(null, "clientName is required", null);
    }

    return cache.computeIfAbsent(key, this::createClient);
  }

  public StorageClient getDefault() {
    String clientName =
        config
            .clientNameForScheme(null)
            .orElseThrow(
                () ->
                    StorageException.invalidRequest(
                        null, "No default storage client configured", null));
    return get(clientName);
  }

  public StorageClient getForUri(URI uri) {
    String clientName =
        config
            .clientNameForUri(uri)
            .orElseThrow(
                () ->
                    StorageException.invalidRequest(
                        null, "No default storage client configured", null));

    return get(clientName);
  }

  public StorageClient getForScheme(String scheme) {
    String clientName =
        config
            .clientNameForScheme(scheme)
            .orElseThrow(
                () ->
                    StorageException.invalidRequest(
                        null, "No default storage client configured", null));

    return get(clientName);
  }

  public boolean has(String clientName) {
    String key = trimToNull(clientName);

    if (key == null) {
      return false;
    }

    return config.clients().containsKey(key);
  }

  private StorageClient createClient(String clientName) {
    Map<String, NamedStorageClientConfig> clients = config.clients();
    NamedStorageClientConfig clientConfig = clients.get(clientName);

    if (clientConfig == null) {
      throw StorageException.invalidRequest(
          null, "No configured storage client found for: " + clientName, null);
    }

    String backend = trimToNull(clientConfig.backend());

    if (backend == null) {
      throw StorageException.invalidRequest(
          null, "backend is required for storage client: " + clientName, null);
    }

    StorageClientProvider selected = providers.get(backend);
    if (selected == null) {
      throw StorageException.invalidRequest(
          null, "No StorageClientProvider found for backend: " + backend, null);
    }
    return selected.create(clientName, clientConfig);
  }

  @Override
  public void close() {
    for (StorageClient client : cache.values()) {
      try {
        client.close();
      } catch (Exception ignored) {
        // Best-effort shutdown cleanup.
      }
    }

    cache.clear();
  }

  private static String trimToNull(String value) {
    if (value == null) {
      return null;
    }

    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }
}
