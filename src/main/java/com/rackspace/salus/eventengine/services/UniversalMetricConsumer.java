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

package com.rackspace.salus.eventengine.services;

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class UniversalMetricConsumer {

  private final EventContextResolver eventContextResolver;
  private final AppProperties appProperties;

  @Autowired
  public UniversalMetricConsumer(
      EventContextResolver eventContextResolver,
      AppProperties appProperties) {
    this.eventContextResolver = eventContextResolver;
    this.appProperties = appProperties;
  }

  /**
   * This method is used by the __listener.topic magic in the KafkaListener
   *
   * @return The topic to consume
   */
  public String getTopic() {
    return appProperties.getMetricsTopic();
  }

  /**
   * This receives a UniversalMetricFrame event from Kafka and passes it on to another service to
   * process it.
   *
   * @param metricFrame The UniversalMetricFrame read from Kafka.
   */
  @KafkaListener(
      // Will be started by EventTaskListener after tasks are loaded
      autoStartup = "false",
      topics = "#{__listener.topic}",
      properties = {
          ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS + "="
              + "com.rackspace.monplat.protocol.UniversalMetricFrameDeserializer"
      })
  public void consumeUniversalMetrics(UniversalMetricFrame metricFrame) {
    log.trace("Consumed metricFrame frame: {}", metricFrame);

    /*
    The list of metrics in a frame are generalized to accommodate both cases of highly
    inter-related metrics that were gathered by a single collector, such as telegraf plugin metrics,
    or could simply be a batch of resource metrics with differing timestamps, metric-groups,
    and tags.

    The following stream operations strive to re-bundle the list of metrics as much as possible
    for the first case scenario.
     */

    final List<GroupedMetric> groupedMetrics = metricFrame.getMetricsList().stream()
        // group them up first by GroupingKey
        .collect(Collectors.groupingBy(
            metric -> new GroupingKey(
                metric.getTimestamp(), metric.getGroup(), metric.getMetadataMap())
        ))
        // and now each grouped list can become an IncomingMetric each
        .entrySet().stream()
        .map(groupedEntry ->
            new GroupedMetric()
                // frame level identifiers
                .setTenantId(metricFrame.getTenantId())
                // grouped identifiers
                .setTimestamp(
                    Instant.ofEpochSecond(
                        groupedEntry.getKey().getTimestamp().getSeconds(),
                        groupedEntry.getKey().getTimestamp().getNanos()
                    )
                )
                .setMetricGroup(groupedEntry.getKey().getMetricGroup())
                .setLabels(groupedEntry.getKey().getMetricTags())
                // metric name-value entries
                .setMetrics(
                    groupedEntry.getValue().stream()
                        .collect(
                            Collectors.toMap(
                                Metric::getName,
                                metric -> {
                                  switch (metric.getValueCase()) {
                                    case FLOAT:
                                      return metric.getFloat();
                                    case INT:
                                      return metric.getInt();
                                    case STRING:
                                      return metric.getString();
                                    case BOOL:
                                      return metric.getBool();
                                    default:
                                      return 0;
                                  }
                                }
                            )
                        )
                )

        )
        .collect(Collectors.toList());

    groupedMetrics
        .forEach(eventContextResolver::process);
  }

  private Instant convertTimestamp(Timestamp timestamp) {
    return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
  }

  @Data
  private static class GroupingKey {

    final Timestamp timestamp;
    final String metricGroup;
    final Map<String, String> metricTags;
  }
}
