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
package software.amazon.s3.analyticsaccelerator.common.telemetry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.*;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/**
 * Represents a datapoint managed by {@link Telemetry} - e.g. {@link Operation} or {@link Metric}
 */
@Getter
@EqualsAndHashCode
public abstract class TelemetryDatapoint {
  /** Datapoint name. Must not be null. */
  @NonNull private final String name;

  /** Metric attributes. Must not be null */
  @NonNull private final Map<String, Attribute> attributes;

  /**
   * Creates a new instance of {@link TelemetryDatapoint}.
   *
   * @param name operation name.
   * @param attributes operation attributes.
   */
  protected TelemetryDatapoint(@NonNull String name, @NonNull Map<String, Attribute> attributes) {
    this.name = name;
    this.attributes = Collections.unmodifiableMap(attributes);
  }

  /**
   * The builder base for {@link TelemetryDatapoint}
   *
   * @param <T> a subtype for {@link TelemetryDatapoint}
   * @param <TBuilder> a subtype for {@link TelemetryDatapoint} builder
   */
  public abstract static class TelemetryDatapointBuilder<
      T extends TelemetryDatapoint, TBuilder extends TelemetryDatapointBuilder<T, TBuilder>> {
    @Getter(AccessLevel.PROTECTED)
    private String name;

    @Getter(AccessLevel.PROTECTED)
    private final Map<String, Attribute> attributes = new HashMap<String, Attribute>();

    /**
     * Sets the operation name.
     *
     * @param name operation name. Must not be null.
     * @return the current instance of {@link TBuilder}.
     */
    public TBuilder name(@NonNull String name) {
      this.name = name;
      return self();
    }

    /**
     * Adds a new attribute to the operation.
     *
     * @param name attribute name. Must not be null.
     * @param value attribute value. Must not be null.
     * @return the current instance of {@link TBuilder}.
     */
    public TBuilder attribute(@NonNull String name, @NonNull Object value) {
      return attribute(Attribute.of(name, value));
    }

    /**
     * Adds a new attribute to the operation.
     *
     * @param attribute attribute to add.
     * @return the current instance of {@link TBuilder}.
     */
    public TBuilder attribute(@NonNull Attribute attribute) {
      // Add the attribute, presuming the is no another attribute by that name
      Attribute existingAttribute = this.attributes.putIfAbsent(attribute.getName(), attribute);
      if (existingAttribute != null) {
        throw new IllegalArgumentException(
            "Attribute with this name already already exists: " + existingAttribute);
      }
      return self();
    }

    /**
     * Returns a strongly typed "self" based on the derived class
     *
     * @return correctly cast 'this'
     */
    @SuppressWarnings("unchecked")
    protected TBuilder self() {
      return (TBuilder) this;
    }

    /**
     * Builds the instance of {@link T}.
     *
     * @return a new instance of {@link T}
     */
    public T build() {
      // Check name - the rest has sensible defaults
      Preconditions.checkNotNull(this.name, "The `name` must be set");
      return buildCore();
    }

    /** @return new instance of whatever this builder builds */
    protected abstract T buildCore();
  }
}
