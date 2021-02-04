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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.mockito.Mockito.verify;

import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.statemachines.MultiStateTransition;
import com.rackspace.salus.event.statemachines.MultiStateTransition.Observation;
import com.rackspace.salus.event.statemachines.StateTransition;
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters.TaskState;
import com.rackspace.salus.telemetry.messaging.EventNotification;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = {EventNotificationProducer.class})
@EnableConfigurationProperties({
    AppProperties.class,
    KafkaTopicProperties.class
})
public class EventNotificationProducerTest {

  @MockBean
  KafkaTemplate<String, EventNotification> kafkaTemplate;

  @Autowired
  EventNotificationProducer eventNotificationProducer;

  @Autowired
  KafkaTopicProperties kafkaTopicProperties;

  @Autowired
  AppProperties appProperties;

  @Test
  public void testHandleStateChange() {
    final String zone = randomAlphabetic(5);
    final String tenantId = randomAlphabetic(10);
    final String topic = randomAlphabetic(5);
    final String message = randomAlphabetic(10);
    final String metricGroup = randomAlphabetic(5);
    final String groupingLabelV = randomAlphabetic(5);

    kafkaTopicProperties.setEventNotifications(topic);

    appProperties.setNotificationTimestampRounding(Duration.ofMinutes(5));
    final Instant timestamp = Instant.parse("2020-12-18T20:11:42Z");
    final Instant timestampRounded = Instant.parse("2020-12-18T20:10:00Z");

    EventEngineTask task = new EventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId);

    List<Entry<String, String>> groupingLabels = List.of(
        Map.entry(randomAlphabetic(5), groupingLabelV)
    );

    GroupedMetric groupedMetric = new GroupedMetric()
        .setTimestamp(timestamp)
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(randomAlphabetic(5), randomAlphabetic(5)))
        .setMetrics(Map.of(randomAlphabetic(5), 42));

    MultiStateTransition<TaskState, String> transition = new MultiStateTransition<TaskState, String>()
        .setOverall(new StateTransition<>(TaskState.OK, TaskState.CRITICAL))
        .setObservations(
            Map.of(zone, new Observation<TaskState>().setState(TaskState.CRITICAL))
        );

    // EXECUTE

    eventNotificationProducer.handleStateChange(
        task,
        groupingLabels,
        groupedMetric,
        transition,
        message
    );

    // VERIFY

    final String notificationId = String.join(
        ":",
        tenantId,
        groupingLabelV,
        task.getId().toString(),
        Long.toString(timestampRounded.getEpochSecond()),
        "CRITICAL"
    );

    verify(kafkaTemplate).send(
        kafkaTopicProperties.getEventNotifications(),
        notificationId,
        new EventNotification()
            .setId(notificationId)
            .setTenantId(task.getTenantId())
            .setTimestamp(timestamp)
            .setTaskId(task.getId().toString())
            .setState("CRITICAL")
            .setPreviousState("OK")
            .setObservations(List.of(
                new EventNotification.Observation()
                    .setZone(zone)
                    .setState("CRITICAL")
            ))
            .setMessage(message)
            .setMetricGroup(metricGroup)
            .setMetrics(groupedMetric.getMetrics())
            .setGroupingLabels(groupingLabels)
            .setLabels(groupedMetric.getLabels())
    );
  }
}