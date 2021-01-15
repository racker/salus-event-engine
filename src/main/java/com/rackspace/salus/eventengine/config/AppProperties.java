/*
 * Copyright 2020 Rackspace US, Inc.
 *
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

package com.rackspace.salus.eventengine.config;

import java.time.Duration;
import javax.validation.constraints.NotBlank;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("salus.event-engine")
@Component
@Data
@Validated
public class AppProperties {

  /**
   * The zone name to use when processing an event task that does not have a zone label configured.
   */
  @NotBlank
  String localZone = "_LOCAL_";

  /**
   * The zone name to use when the zone label declared by an event task is not present in a
   * processed metric.
   */
  @NotBlank
  String unknownZone = "_UNKNOWN_";

  Duration taskLoadingInitialDelay = Duration.ofMinutes(1);

  @NotBlank
  String metricsTopic;

  /**
   * The notification ID includes a timestamp that is rounded down to this duration to eanble
   * time-window correlation.
   */
  Duration notificationTimestampRounding = Duration.ofMinutes(5);
}
