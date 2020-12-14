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

import com.rackspace.salus.event.processor.EventProcessorContext;
import com.rackspace.salus.event.processor.EventProcessorContextBuilder;
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@Slf4j
public class EventContextResolver {

  private final ConcurrentHashMap<TasksKey, Set<EventEngineTask>> tasks = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<UUID, TasksKey> taskKeysById = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<ContextKey, EventProcessorContext> contexts = new ConcurrentHashMap<>();
  /**
   * Enables reverse lookup-removal of contexts when the task is unregistered
   */
  private final ConcurrentHashMap<UUID/*taskId*/, List<ContextKey>> contextsByTask = new ConcurrentHashMap<>();

  private final EventProcessorAdapter eventProcessorAdapter;

  private final AppProperties appProperties;

  @Autowired
  public EventContextResolver(MeterRegistry meterRegistry,
                              EventProcessorAdapter eventProcessorAdapter,
                              AppProperties appProperties) {
    this.eventProcessorAdapter = eventProcessorAdapter;
    this.appProperties = appProperties;

    meterRegistry.gaugeMapSize("taskContexts", Collections.emptyList(), contexts);
  }

  /**
   * For unit testing
   */
  boolean hasTask(EventEngineTask task) {
    final Set<EventEngineTask> tasksEntry = tasks
        .get(new TasksKey(task.getTenantId(), task.getTaskParameters().getMetricGroup()));
    return tasksEntry != null && tasksEntry.contains(task);
  }

  /**
   * For unit testing
   */
  List<EventProcessorContext> getContextsForTask(EventEngineTask task) {
    final List<ContextKey> contextKeys = contextsByTask.get(task.getId());
    return contextKeys == null ? Collections.emptyList() :
        contextKeys.stream()
            .map(contexts::get)
            .collect(Collectors.toList());
  }

  /**
   * For unit testing
   */
  int getTaskTrackingCount() {
    return tasks.size();
  }

  /**
   * For unit testing
   */
  int getContextCount() {
    return contexts.size();
  }

  public void registerTask(EventEngineTask task) {
    log.debug("Registering task={}", task);
    final TasksKey key = new TasksKey(
        task.getTenantId(), task.getTaskParameters().getMetricGroup());

    tasks.computeIfAbsent(key, unused -> new CopyOnWriteArraySet<>())
        .add(task);

    taskKeysById.put(task.getId(), key);
  }

  public void updateTask(EventEngineTask task) {
    log.debug("Updating task={}", task);
    final TasksKey key = new TasksKey(
        task.getTenantId(), task.getTaskParameters().getMetricGroup());

    final Set<EventEngineTask> tasks = this.tasks.get(key);
    if (tasks == null) {
      throw new IllegalArgumentException("Cannot update task that is not registered");
    } else {
      // replaces task due to being a set
      tasks.add(task);
    }
  }

  public void unregisterTask(UUID taskId) {
    log.debug("Unregistering taskId={}", taskId);

    final TasksKey tasksKey = taskKeysById.remove(taskId);

    if (tasksKey != null) {
      final Set<EventEngineTask> entry = tasks.get(tasksKey);
      if (entry != null) {
        entry.removeIf(task -> task.getId().equals(taskId));
        if (entry.isEmpty()) {
          tasks.remove(tasksKey);
        }
      }
    }

    final List<ContextKey> contextKeys = contextsByTask.remove(taskId);
    if (contextKeys != null) {
      contextKeys.forEach(contexts::remove);
    }
  }

  /**
   * With the given metric, see if it matches any configured event tasks and if it does
   * process the metric through the associated context's state machine.
   */
  public void process(GroupedMetric metric) {
    // First lookup by general task key
    final Set<EventEngineTask> candidateTasks = tasks
        .get(new TasksKey(metric.getTenantId(), metric.getMetricGroup()));

    if (candidateTasks != null) {
      candidateTasks.stream()
          // ...then narrow down by label selectors
          .filter(task -> matchesLabelSelector(task.getTaskParameters(), metric.getLabels()))
          .forEach(task -> {

            final List<Entry<String, String>> groupingLabels = buildGroupingLabels(metric, task);

            // Lookup the context
            final EventProcessorContext eventProcessorContext = contexts.computeIfAbsent(
                new ContextKey(task.getId(), groupingLabels),
                contextKey -> {
                  // ... store reverse mapping from task
                  contextsByTask.computeIfAbsent(task.getId(), key -> new ArrayList<>())
                      .add(contextKey);
                  // ... and create context when first accessed
                  return EventProcessorContextBuilder.fromTask(task);
                }
            );

            // Process the next step
            eventProcessorAdapter.process(
                eventProcessorContext,
                groupingLabels,
                resolveZone(metric, task),
                metric
            );
          });
    }
  }

  private List<Entry<String, String>> buildGroupingLabels(GroupedMetric metric, EventEngineTask task) {
    // Determine grouping label key-values from concatenation of ...
    final List<Entry<String, String>> groupingLabels = Stream.concat(
        // ...the label selectors
        emptyMapIfNull(
            task.getTaskParameters().getLabelSelector()).entrySet().stream()
            // ...sorted by key
            .sorted(Entry.comparingByKey()),
        // ...grouping labels configured on the task
        emptyListIfNull(task.getTaskParameters().getGroupBy()).stream()
            .map(s -> Map.entry(s, metric.getLabels().get(s)))
    )
        .collect(Collectors.toList());
    return groupingLabels;
  }

  private String resolveZone(GroupedMetric metric, EventEngineTask task) {
    final String zoneLabel = task.getTaskParameters().getZoneLabel();
    if (StringUtils.hasText(zoneLabel)) {
      final String zone = metric.getLabels().get(zoneLabel);
      return StringUtils.hasText(zone) ? zone : appProperties.getUnknownZone();
    } else {
      return appProperties.getLocalZone();
    }
  }

  private boolean matchesLabelSelector(EventEngineTaskParameters taskParameters,
                                       Map<String, String> metricTags) {
    // unset or empty label selectors means "match all"
    if (taskParameters.getLabelSelector() == null || taskParameters.getLabelSelector().isEmpty()) {
      return true;
    }

    return taskParameters.getLabelSelector().entrySet().stream()
        .allMatch(selector -> {
          final String value = metricTags.get(selector.getKey());
          return value != null && value.equals(selector.getValue());
        });
  }

  private static List<String> emptyListIfNull(List<String> input) {
    return input != null ? input : Collections.emptyList();
  }

  private static Map<String, String> emptyMapIfNull(Map<String, String> input) {
    return input != null ? input : Collections.emptyMap();
  }

  @Data
  private static final class TasksKey {

    final String tenantId;
    final String metricGroup;
  }

  @Data
  private static final class ContextKey {

    final UUID taskId;
    final List<Entry<String, String>> groupingTags;
  }
}
