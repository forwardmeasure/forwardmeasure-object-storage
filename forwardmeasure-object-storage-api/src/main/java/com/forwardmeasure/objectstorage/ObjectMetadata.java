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

/** Metadata associated with a storage object. */
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

  /** Provider version/generation identifier when supported. */
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

  /** Provider-specific metadata/fields that don’t cleanly map to standard headers. */
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
