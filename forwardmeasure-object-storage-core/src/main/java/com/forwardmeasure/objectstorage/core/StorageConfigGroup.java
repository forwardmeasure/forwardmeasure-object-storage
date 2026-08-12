package com.forwardmeasure.objectstorage.core;

import java.net.URI;
import java.util.Map;
import java.util.Optional;

/**
 * Prefix-free reusable storage configuration group.
 *
 * This interface is intentionally NOT annotated with @ConfigMapping.
 *
 * Consuming applications should compose it into their own root config mapping,
 * for example:
 *
 * <pre>{@code
 * @ConfigMapping(prefix = "application")
 * public interface ApplicationConfig {
 *     StorageConfigGroup storage();
 * }
 * }</pre>
 *
 *                       That allows the same storage client library to be
 *                       mounted under different
 *                       roots such as:
 *
 *                       application.storage.*
 *                       entity-intelligence.storage.*
 *                       evidence.storage.*
 */
public interface StorageConfigGroup {

    /**
     * Name of the default storage client.
     *
     * Example:
     *
     * application.storage.default-client=evidence-store
     */
    Optional<String> defaultClient();

    /**
     * Named storage client definitions.
     *
     * Example:
     *
     * application.storage.clients.evidence-store.backend=gcs
     * application.storage.clients.s3-evidence-store.backend=s3
     */
    Map<String, NamedStorageClientConfig> clients();

    /**
     * URI scheme to named storage client mappings.
     *
     * Example:
     *
     * application.storage.scheme-clients.gs=evidence-store
     * application.storage.scheme-clients.gcs=evidence-store
     * application.storage.scheme-clients.s3=s3-evidence-store
     * application.storage.scheme-clients.abfs=azure-evidence-store
     */
    Map<String, String> schemeClients();

    /**
     * URI host/path prefix to named storage client mappings.
     *
     * More specific keys win. Keys may be either full storage URI prefixes
     * such as {@code s3://tenant-a-bucket/private}, or host/path prefixes
     * such as {@code tenant-a-bucket/private}. Host/path prefixes are matched
     * against {@link URI#getHost()} and {@link URI#getPath()}.
     *
     * This allows one scheme to use different named clients, and therefore
     * different credentials, based on the concrete storage URI.
     */
    Map<String, String> uriClients();

    /**
     * Resolves a named storage client from a URI.
     *
     * URI-prefix mappings take precedence over scheme mappings. If no
     * URI-specific mapping exists, this falls back to
     * {@link #clientNameForScheme(String)}.
     */
    default Optional<String> clientNameForUri(URI uri) {
        if (uri == null) {
            return clientNameForScheme(null);
        }

        Map<String, String> uriMappings = Optional.ofNullable(uriClients()).orElse(Map.of());

        Optional<String> uriMatch = uriMappings.entrySet().stream()
                .filter(e -> uriMatches(uri, e.getKey()))
                .max((a, b) -> Integer.compare(
                        a.getKey().length(), b.getKey().length()))
                .map(Map.Entry::getValue)
                .map(StorageConfigGroup::trimToNull)
                .filter(value -> value != null);

        return uriMatch.or(() -> clientNameForScheme(uri.getScheme()));
    }

    /**
     * Resolves a named storage client from a URI scheme.
     *
     * This method intentionally does not lowercase or otherwise normalize
     * scheme or client names. The configuration author controls the mapping
     * keys and values exactly.
     *
     * If no scheme is provided, or no scheme-specific mapping exists, this
     * method returns the configured default client.
     */
    default Optional<String> clientNameForScheme(String scheme) {
        if (scheme != null && !scheme.isBlank()) {
            String lookupScheme = scheme.trim();
            String clientName = Optional.ofNullable(schemeClients())
                    .orElse(Map.of())
                    .get(lookupScheme);
            String resolved = trimToNull(clientName);
            if (resolved != null) {
                return Optional.of(resolved);
            }
        }
        return Optional.ofNullable(defaultClient())
                .orElse(Optional.empty())
                .map(StorageConfigGroup::trimToNull)
                .filter(value -> value != null);
    }

    private static boolean uriMatches(URI uri, String key) {
        String candidate = trimToNull(key);
        if (candidate == null) {
            return false;
        }

        if (candidate.contains("://")) {
            return fullUriPrefixMatches(storageUriPrefix(uri), candidate);
        }

        return hostPathPrefixMatches(uri, candidate);
    }

    private static boolean fullUriPrefixMatches(String uriPrefix, String key) {
        String normalizedKey = stripTrailingSlash(key);
        return uriPrefix.equals(normalizedKey)
                || uriPrefix.startsWith(normalizedKey + "/");
    }

    private static boolean hostPathPrefixMatches(URI uri, String key) {
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            return false;
        }

        int slash = key.indexOf('/');
        if (slash < 0) {
            return hostSuffixMatches(host, key);
        }

        String keyHost = key.substring(0, slash);
        String keyPath = stripTrailingSlash(key.substring(slash));
        String uriPath = stripTrailingSlash(Optional.ofNullable(uri.getPath()).orElse(""));

        return hostSuffixMatches(host, keyHost)
                && (uriPath.equals(keyPath) || uriPath.startsWith(keyPath + "/"));
    }

    private static boolean hostSuffixMatches(String host, String keyHost) {
        return host.equals(keyHost) || host.endsWith("." + keyHost);
    }

    private static String storageUriPrefix(URI uri) {
        if (uri.getScheme() == null || uri.getHost() == null) {
            return "";
        }
        String path = Optional.ofNullable(uri.getPath()).orElse("");
        return stripTrailingSlash(uri.getScheme() + "://" + uri.getHost() + path);
    }

    private static String stripTrailingSlash(String value) {
        if (value == null || value.length() <= 1 || !value.endsWith("/")) {
            return value;
        }
        return value.substring(0, value.length() - 1);
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }

        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
