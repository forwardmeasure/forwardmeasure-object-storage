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

import java.util.Optional;

/**
 * Whole-object checksums the provider records for an object, in the provider's own encoding: base64
 * of the big-endian CRC32C value and of the MD5 digest. A provider leaves a value null when it does
 * not hold one for the object - GCS composite objects have no MD5, S3 holds a full-object CRC32C
 * only when the object was written with one, Azure holds only an MD5.
 */
public record ObjectChecksums(String crc32c, String md5) {

  public static final ObjectChecksums NONE = new ObjectChecksums(null, null);

  public Optional<String> crc32cOptional() {
    return Optional.ofNullable(crc32c);
  }

  public Optional<String> md5Optional() {
    return Optional.ofNullable(md5);
  }
}
