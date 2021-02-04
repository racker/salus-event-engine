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

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.statemachines.MultiStateTransition;
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.TaskState;
import com.rackspace.salus.telemetry.messaging.EventNotification;
import com.rackspace.salus.telemetry.messaging.EventNotification.Observation;
import com.rackspace.salus.telemetry.messaging.KafkaMessageKeyBuilder;
import java.time.Instant;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventNotificationProducer {

  private static final String ID_DELIM = ":";
  private final KafkaTemplate<String, EventNotification> kafkaTemplate;
  private final AppProperties appProperties;
  private final KafkaTopicProperties kafkaTopicProperties;

  private final TemporalAdjuster timeRounding;

  @Autowired
  public EventNotificationProducer(KafkaTemplate<String, EventNotification> kafkaTemplate,
                                   AppProperties appProperties,
                                   KafkaTopicProperties kafkaTopicProperties) {
    this.kafkaTemplate = kafkaTemplate;
    this.appProperties = appProperties;
    this.kafkaTopicProperties = kafkaTopicProperties;

    timeRounding = temporal ->
        temporal
            .with(ChronoField.NANO_OF_SECOND, 0)
            .with(
                ChronoField.INSTANT_SECONDS, round(temporal.getLong(ChronoField.INSTANT_SECONDS)));
  }

  private long round(long seconds) {
    return seconds - (seconds % appProperties.getNotificationTimestampRounding().getSeconds());
  }

  public void handleStateChange(EventEngineTask task, List<Entry<String, String>> groupingLabels,
                                GroupedMetric groupedMetric,
                                MultiStateTransition<TaskState, String> transition,
                                String message) {
    log.trace("Handling state change for task={} due to metric={} where transition={}",
        task, groupedMetric, transition);

    if (transition.getOverall().getFrom() == null &&
      transition.getOverall().getTo() == TaskState.OK &&
      !appProperties.isNotifyOnInitialOk()) {
      log.trace("Ignoring initial transition to OK for task={} due to metric={}",
          task, groupedMetric
      );
      return;
    }

    final Instant timestamp = groupedMetric.getTimestamp();
    final String tenantId = task.getTenantId();
    final String taskId = task.getId().toString();

    final StringBuilder id = new StringBuilder(tenantId);
    if (!groupingLabels.isEmpty()) {
      id.append(ID_DELIM).append(
          groupingLabels.stream()
              .map(Entry::getValue)
              .collect(Collectors.joining(ID_DELIM))
      );
    }
    id.append(ID_DELIM).append(taskId);
    id.append(ID_DELIM).append(timestamp.with(timeRounding).getEpochSecond());
    id.append(ID_DELIM).append(transition.getOverall().getTo());

    final EventNotification notification = new EventNotification()
        .setId(id.toString())
        .setTenantId(tenantId)
        .setTimestamp(timestamp)
        .setTaskId(taskId)
        .setState(transition.getOverall().getTo().toString())
        .setPreviousState(transition.getOverall().getFrom() != null ?
            transition.getOverall().getFrom().toString() : null)
        .setObservations(convertObservations(transition.getObservations()))
        .setMessage(message)
        .setMetricGroup(groupedMetric.getMetricGroup())
        .setMetrics(groupedMetric.getMetrics())
        .setGroupingLabels(groupingLabels)
        .setLabels(groupedMetric.getLabels());

    kafkaTemplate.send(
        kafkaTopicProperties.getEventNotifications(),
        KafkaMessageKeyBuilder.buildMessageKey(notification),
        notification
    );
  }

  private List<Observation> convertObservations(
      Map<String, MultiStateTransition.Observation<TaskState>> observations) {
    return observations.entrySet().stream()
        .map(entry -> new Observation()
            .setZone(entry.getKey())
            .setState(entry.getValue().getState().toString())
        )
        .collect(Collectors.toList());
  }
}
