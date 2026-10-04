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
import java.util.Optional;

public record DefaultObjectMetadata(
    String contentTypeValue,
    long contentLength,
    String contentTagValue,
    String versionIdValue,
    Instant lastModifiedValue,
    Map<String, String> userMetadataMap,
    Map<String, String> systemMetadataMap,
    Map<String, String> attributesMap,
    String contentEncodingValue,
    String cacheControlValue,
    String contentDispositionValue,
    String contentLanguageValue)
    implements ObjectMetadata {

  public DefaultObjectMetadata {
    if (contentLength < 0) throw new IllegalArgumentException("contentLength must be >= 0");
    userMetadataMap = userMetadataMap == null ? Map.of() : Map.copyOf(userMetadataMap);
    systemMetadataMap = systemMetadataMap == null ? Map.of() : Map.copyOf(systemMetadataMap);
    attributesMap = attributesMap == null ? Map.of() : Map.copyOf(attributesMap);
  }

  @Override
  public Optional<String> contentType() {
    return Optional.ofNullable(contentTypeValue);
  }

  @Override
  public long contentLength() {
    return contentLength;
  }

  @Override
  public Optional<String> contentTag() {
    return Optional.ofNullable(contentTagValue);
  }

  @Override
  public Optional<String> versionId() {
    return Optional.ofNullable(versionIdValue);
  }

  @Override
  public Optional<Instant> lastModified() {
    return Optional.ofNullable(lastModifiedValue);
  }

  @Override
  public Map<String, String> userMetadata() {
    return userMetadataMap;
  }

  @Override
  public Map<String, String> systemMetadata() {
    return systemMetadataMap;
  }

  @Override
  public Map<String, String> attributes() {
    return attributesMap;
  }

  @Override
  public Optional<String> contentEncoding() {
    return Optional.ofNullable(contentEncodingValue);
  }

  @Override
  public Optional<String> cacheControl() {
    return Optional.ofNullable(cacheControlValue);
  }

  @Override
  public Optional<String> contentDisposition() {
    return Optional.ofNullable(contentDispositionValue);
  }

  @Override
  public Optional<String> contentLanguage() {
    return Optional.ofNullable(contentLanguageValue);
  }
}
