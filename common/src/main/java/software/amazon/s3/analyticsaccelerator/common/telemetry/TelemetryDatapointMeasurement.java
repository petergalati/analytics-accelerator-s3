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

import lombok.*;
import software.amazon.s3.analyticsaccelerator.common.Preconditions;

/** Represents a measurements for a given {@link TelemetryDatapoint} */
@Getter
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@EqualsAndHashCode
public abstract class TelemetryDatapointMeasurement {
  /** Metric */
  @NonNull private final TelemetryDatapoint datapoint;

  /** Wall clock time corresponding to metric. */
  private final long epochTimestampNanos;

  /**
   * The actual measurement
   *
   * @return the actual measurement
   */
  public double getValue() {
    return this.getValueCore();
  }

  /**
   * The actual measurement
   *
   * @return the actual measurement
   */
  protected abstract double getValueCore();

  /**
   * Returns the String representation of the {@link TelemetryDatapoint}
   *
   * @param telemetryFormat the {@link TelemetryFormat} that controls the output format
   * @param epochFormatter the {@link EpochFormatter} that controls the formatting of epochs
   * @return the String representation of the {@link TelemetryDatapoint}.
   */
  public abstract String toString(
      @NonNull TelemetryFormat telemetryFormat, @NonNull EpochFormatter epochFormatter);

  /**
   * The builder base for {@link TelemetryDatapoint}
   *
   * @param <TBuilder> a subtype for {@link TelemetryDatapoint} builder
   */
  public abstract static class TelemetryDatapointMeasurementBuilder<
      T extends TelemetryDatapointMeasurement,
      TBuilder extends TelemetryDatapointMeasurementBuilder<T, TBuilder>> {
    protected static final long UNSET_NANOS = Long.MIN_VALUE;

    @Getter(AccessLevel.PROTECTED)
    private long epochTimestampNanos = UNSET_NANOS;

    /**
     * Sets epoch timestamp.
     *
     * @param epochTimestampNanos epoch timestamp.
     * @return the current instance of {@link OperationMeasurement.OperationMeasurementBuilder}.
     */
    public TBuilder epochTimestampNanos(long epochTimestampNanos) {
      this.epochTimestampNanos = epochTimestampNanos;
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
      Preconditions.checkArgument(
          this.epochTimestampNanos >= 0, "The `epochTimestampNanos` must be set and non-negative.");
      return buildCore();
    }

    /** @return new instance of whatever this builder builds */
    protected abstract T buildCore();
  }
}
