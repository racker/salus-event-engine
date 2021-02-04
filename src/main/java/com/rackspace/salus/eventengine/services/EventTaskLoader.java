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

import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.repositories.EventEngineTaskRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Slf4j
public class EventTaskLoader {

  private final EventEngineTaskRepository taskRepository;

  @Autowired
  public EventTaskLoader(EventEngineTaskRepository taskRepository) {
    this.taskRepository = taskRepository;
  }

  @Transactional(readOnly = true)
  public void loadAll(Consumer<EventEngineTask> handler) {
    final Instant startTime = Instant.now();
    try (Stream<EventEngineTask> taskStream = taskRepository.streamAll()) {
      taskStream.forEach(handler);
    }

    log.info("Loaded tasks in duration={}", Duration.between(startTime, Instant.now()));
  }
}
