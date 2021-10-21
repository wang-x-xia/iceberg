/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.dell;

import com.emc.object.s3.S3Client;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link java.io.Externalizable} FileIO of ECS S3 object client.
 */
public class EcsFileIO implements FileIO, Externalizable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(EcsFileIO.class);

  private Map<String, String> properties;
  private S3Client client;
  private final AtomicBoolean closed = new AtomicBoolean(true);

  @Override
  public void initialize(Map<String, String> properties) {
    if (closed.compareAndSet(true, false)) {
      this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
      this.client = EcsClientFactory.create(properties);
    } else {
      log.error("Try to re-initialized the properties");
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    checkOpen();
    return new EcsFile(client, path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    checkOpen();
    return new EcsFile(client, path);
  }

  @Override
  public void deleteFile(String path) {
    checkOpen();
    EcsURI uri = LocationUtils.checkAndParseLocation(path);
    client.deleteObject(uri.getBucket(), uri.getName());
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(properties);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    @SuppressWarnings("unchecked")
    Map<String, String> properties = (Map<String, String>) in.readObject();
    initialize(properties);
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      client.destroy();
      log.info("S3 Client in FileIO is closed");
    }
  }

  private void checkOpen() {
    if (closed.get()) {
      throw new IllegalStateException("Closed FileIO");
    }
  }
}
