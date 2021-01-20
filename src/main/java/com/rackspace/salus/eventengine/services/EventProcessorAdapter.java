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

import com.rackspace.salus.event.processor.EventProcessor;
import com.rackspace.salus.event.processor.EventProcessorContext;
import com.rackspace.salus.event.processor.EventProcessorInput;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import java.util.List;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class EventProcessorAdapter {

  private final EventProcessor eventProcessor;
  private final EventNotificationProducer eventNotificationProducer;

  @Autowired
  public EventProcessorAdapter(EventProcessor eventProcessor,
                               EventNotificationProducer eventNotificationProducer) {
    this.eventProcessor = eventProcessor;
    this.eventNotificationProducer = eventNotificationProducer;
  }

  public void process(EventProcessorContext context,
                      List<Entry<String, String>> groupingLabels,
                      String zone, GroupedMetric groupedMetric) {
    log.trace("Processing metric={} in context={} via event processor",
        groupedMetric, context);

    eventProcessor.process(
        context,
        new EventProcessorInput(
            groupedMetric.getTimestamp(),
            zone,
            groupedMetric.getMetrics()
        ),
        (transition, message) -> eventNotificationProducer.handleStateChange(
            context.getTask(), groupingLabels, groupedMetric, transition, message)
    );
  }
}
