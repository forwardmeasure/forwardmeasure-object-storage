package com.forwardmeasure.objectstorage;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Provider-agnostic description of a bucket/container.
 *
 * <p>Notes: - "name" is required. - Other fields may be unavailable depending on provider and
 * permissions. - tags/attributes are optional maps: - tags: user-facing labels (when supported) -
 * attributes: provider-specific key/values (region, storage class defaults, etc.)
 */
public record Bucket(
    String name,
    Instant createdAt,
    String location,
    Map<String, String> tags,
    Map<String, String> attributes) {

  public Bucket {
    Objects.requireNonNull(name, "name");
    tags = (tags == null) ? Map.of() : Map.copyOf(tags);
    attributes = (attributes == null) ? Map.of() : Map.copyOf(attributes);
  }

  public Optional<Instant> createdAtOptional() {
    return Optional.ofNullable(createdAt);
  }

  public Optional<String> locationOptional() {
    return Optional.ofNullable(location);
  }
}
