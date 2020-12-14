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
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.messaging.TaskChangeEvent;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventTaskLoader {

  private final ThreadPoolTaskExecutor executor;
  private final EventEngineTaskRepository taskRepository;
  private final KafkaTopicProperties kafkaTopicProperties;
  private final AppProperties appProperties;
  private final EventContextResolver eventContextResolver;

  @Autowired
  public EventTaskLoader(ThreadPoolTaskExecutor executor,
                         EventEngineTaskRepository taskRepository,
                         KafkaTopicProperties kafkaTopicProperties,
                         AppProperties appProperties,
                         EventContextResolver eventContextResolver) {
    this.executor = executor;
    this.taskRepository = taskRepository;
    this.kafkaTopicProperties = kafkaTopicProperties;
    this.appProperties = appProperties;
    this.eventContextResolver = eventContextResolver;
  }

  public String getTopic() {
    return kafkaTopicProperties.getTaskChanges();
  }

  @KafkaListener(topicPartitions = @TopicPartition(
      topic = "#{__listener.topic}",
      partitions = "#{@partitionFinder.partitions(__listener.topic)}"
  ),
    properties = {
        ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS + "=" +
            "org.springframework.kafka.support.serializer.JsonDeserializer"
    }
  )
  public void handleChangeEvent(TaskChangeEvent event) {
    taskRepository.findById(event.getTaskId())
        .ifPresentOrElse(
            eventContextResolver::updateTask,
            () -> eventContextResolver.unregisterTask(event.getTaskId())
        );
  }

  @PostConstruct
  public void init() {
    executor.execute(this::loadTasks,
        appProperties.getTaskLoadingInitialDelay().toMillis());
  }

  void loadTasks() {
    log.debug("Loading tasks");

    final Instant startTime = Instant.now();
    try (Stream<EventEngineTask> taskStream = taskRepository.streamAll()) {
      taskStream
          .forEach(eventContextResolver::registerTask);
    }

    log.info("Loaded tasks in duration={}", Duration.between(startTime, Instant.now()));
  }
}
