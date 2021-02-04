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
import com.rackspace.salus.telemetry.messaging.TaskChangeEvent;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.time.Instant;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventTaskListener {

  private final ThreadPoolTaskScheduler scheduler;
  private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  private final EventEngineTaskRepository taskRepository;
  private final EventTaskLoader eventTaskLoader;
  private final AppProperties appProperties;
  private final KafkaTopicProperties kafkaTopicProperties;
  private final EventContextResolver eventContextResolver;
  private final String appName;
  private final String ourHostName;

  @Autowired
  public EventTaskListener(ThreadPoolTaskScheduler scheduler,
                           KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry,
                           EventEngineTaskRepository taskRepository,
                           EventTaskLoader eventTaskLoader,
                           AppProperties appProperties,
                           KafkaTopicProperties kafkaTopicProperties,
                           EventContextResolver eventContextResolver,
                           @Value("spring.application.name") String appName,
                           @Value("${localhost.name}") String ourHostName) {
    this.scheduler = scheduler;
    this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    this.taskRepository = taskRepository;
    this.eventTaskLoader = eventTaskLoader;
    this.appProperties = appProperties;
    this.kafkaTopicProperties = kafkaTopicProperties;
    this.eventContextResolver = eventContextResolver;
    this.appName = appName;
    this.ourHostName = ourHostName;
  }

  public String getTopic() {
    return kafkaTopicProperties.getTaskChanges();
  }

  @SuppressWarnings("unused") // used in SpEL
  public String getGroupId() {
    // consume in a "broadcast-manner" so each instance of this application will act
    // as its own consumer group
    return String.join("-",
        appName,
        appProperties.getEnvironment(),
        ourHostName);
  }

  @KafkaListener(autoStartup = "false",
      properties = {
          // always start with newest events since init loaded latest from DB
          "spring.kafka.consumer.auto-offset-reset=latest"
      },
      topics = "#{__listener.topic}",
      groupId = "#{__listener.groupId}"
  )
  public void handleChangeEvent(TaskChangeEvent event) {
    taskRepository.findById(event.getTaskId())
        .ifPresentOrElse(
            eventContextResolver::registerOrUpdateTask,
            () -> eventContextResolver.unregisterTask(event.getTaskId())
        );
  }

  @PostConstruct
  public void init() {
    log.debug("Scheduling task loading to run in {}", appProperties.getTaskLoadingInitialDelay());
    scheduler.schedule(
        this::loadTasks,
        Instant.now().plus(appProperties.getTaskLoadingInitialDelay())
    );
  }

  void loadTasks() {
    log.debug("Loading tasks");
    eventTaskLoader.loadAll(eventContextResolver::registerOrUpdateTask);

    log.debug("Starting Kafka listeners");
    kafkaListenerEndpointRegistry.getAllListenerContainers()
        .forEach(messageListenerContainer -> {
          log.debug("Starting {}", messageListenerContainer.getListenerId());
          messageListenerContainer.start();
        });
  }
}
