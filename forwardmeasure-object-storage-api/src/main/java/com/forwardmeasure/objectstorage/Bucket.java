/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
