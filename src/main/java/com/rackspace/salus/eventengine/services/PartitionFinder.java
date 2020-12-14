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

import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

@Component
public class PartitionFinder {

  private final ConsumerFactory<?,?> consumerFactory;

  @Autowired
  public PartitionFinder(ConsumerFactory<?,?> consumerFactory) {
    this.consumerFactory = consumerFactory;
  }

  public String[] partitions(String topic) {
    try (Consumer<?,?> consumer = consumerFactory.createConsumer()) {
      return consumer.partitionsFor(topic).stream()
          .map(partitionInfo -> Integer.toString(partitionInfo.partition()))
          .toArray(String[]::new);
    }
  }
}
