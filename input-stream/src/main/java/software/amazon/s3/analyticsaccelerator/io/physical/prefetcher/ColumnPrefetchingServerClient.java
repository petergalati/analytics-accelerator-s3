/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package software.amazon.s3.analyticsaccelerator.io.physical.prefetcher;

import java.io.IOException;
import java.util.Set;
import okhttp3.*;
import org.json.JSONArray;
import org.json.JSONObject;

public class ColumnPrefetchingServerClient {
  private final OkHttpClient client;
  private final String serverUrl;

  public ColumnPrefetchingServerClient(OkHttpClient client, String serverUrl) {
    this.client = client;
    this.serverUrl = serverUrl;
  }

  public Response prefetchColumns(String bucket, String prefix, Set<String> columns)
      throws IOException {
    JSONObject json = new JSONObject();
    json.put("bucket", bucket);
    json.put("prefix", prefix);
    json.put("columns", new JSONArray(columns));

    RequestBody body =
        RequestBody.create(json.toString(), MediaType.parse("application/json; charset=utf-8"));

    Request request = new Request.Builder().url(serverUrl + "/prefetch").post(body).build();

    return client.newCall(request).execute();
  }
}
