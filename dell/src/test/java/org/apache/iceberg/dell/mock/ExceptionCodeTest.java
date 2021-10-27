/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.dell.mock;

import com.emc.object.Range;
import com.emc.object.s3.S3Exception;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Verify the error codes between real client and mock client.
 */
public class ExceptionCodeTest {

  @Rule
  public EcsS3MockRule rule = EcsS3MockRule.create();

  @Test
  public void checkExceptionCode() {
    String object = "test";
    assertS3Exception("Append absent object", 404, "NoSuchKey",
        () -> rule.getClient().appendObject(rule.getBucket(), object, "abc".getBytes()));
    assertS3Exception("Get object", 404, "NoSuchKey",
        () -> rule.getClient().readObjectStream(rule.getBucket(), object, Range.fromOffset(0)));
  }

  public void assertS3Exception(String message, int httpCode, String errorCode, Runnable task) {
    try {
      task.run();
      fail(message + ", expect s3 exception");
    } catch (S3Exception e) {
      assertEquals(message + ", http code", httpCode, e.getHttpCode());
      assertEquals(message + ", error code", errorCode, e.getErrorCode());
    }
  }
}