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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.rackspace.salus.event.processor.EventProcessorContext;
import com.rackspace.salus.eventengine.config.AppProperties;
import com.rackspace.salus.eventengine.model.GroupedMetric;
import com.rackspace.salus.telemetry.entities.EventEngineTask;
import com.rackspace.salus.telemetry.entities.EventEngineTaskParameters;
import com.rackspace.salus.telemetry.entities.subtype.SalusEventEngineTask;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EventContextResolverTest {

  @Mock
  EventProcessorAdapter eventProcessorAdapter;

  @Captor
  ArgumentCaptor<EventProcessorContext> eventProcessorContextArg;

  private final AppProperties appProperties = new AppProperties();
  private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

  @Test
  public void testRegisterAndUnregisterTask() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            .setLabelSelector(Map.of("resource_id", resourceId))
        );

    resolver.registerTask(task);

    assertThat(resolver.getTaskTrackingCount()).isEqualTo(1);
    assertThat(resolver.getContextCount()).isEqualTo(0);
    assertThat(resolver.getContextsForTask(task)).isEmpty();
    assertThat(resolver.hasTask(task)).isTrue();

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of("resource_id", resourceId));

    resolver.process(metric);

    assertThat(resolver.getContextCount()).isEqualTo(1);
    assertThat(resolver.getContextsForTask(task))
        .hasSize(1)
        .anySatisfy(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
        });

    verify(eventProcessorAdapter).process(
        argThat(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
          return true;
        }),
        eq(List.of(Map.entry("resource_id", resourceId))),
        eq(appProperties.getLocalZone()),
        eq(metric)
    );

    resolver.unregisterTask(task.getId());
    assertThat(resolver.getTaskTrackingCount()).isEqualTo(0);
    assertThat(resolver.getContextCount()).isEqualTo(0);
    assertThat(resolver.getContextsForTask(task)).isEmpty();
    assertThat(resolver.hasTask(task)).isFalse();
  }

  @Test
  public void testProcess_noLabelSelectors_noGroupBy_matches() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
        );

    resolver.registerTask(task);

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of("resource_id", resourceId));

    resolver.process(metric);

    verify(eventProcessorAdapter).process(
        argThat(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
          return true;
        }),
        eq(List.of()),
        eq(appProperties.getLocalZone()),
        eq(metric)
    );
  }

  @Test
  public void testProcess_noLabelSelectors_noGroupBy_noMatches() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
        );

    resolver.registerTask(task);

    resolver.process(new GroupedMetric()
        .setTenantId("not" + tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of("resource_id", resourceId)));

    resolver.process(new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup("not" + metricGroup)
        .setLabels(Map.of("resource_id", resourceId)));

    verifyNoInteractions(eventProcessorAdapter);
  }

  @Test
  public void testProcess_noGroupBy_matches() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);
    final String monitorId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            .setLabelSelector(Map.of(
                "resource_id", resourceId,
                "monitor_id", monitorId
            ))
        );

    resolver.registerTask(task);

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(
            "resource_id", resourceId,
            "monitor_id", monitorId
        ));

    resolver.process(metric);

    verify(eventProcessorAdapter).process(
        argThat(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
          return true;
        }),
        eq(List.of(Map.entry("monitor_id", monitorId), Map.entry("resource_id", resourceId))),
        eq(appProperties.getLocalZone()),
        eq(metric)
    );
  }

  @Test
  public void testProcess_noGroupBy_noMatches() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);
    final String monitorId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            .setLabelSelector(Map.of(
                "resource_id", resourceId,
                "monitor_id", monitorId
            ))
        );

    resolver.registerTask(task);

    // missing monitor_id
    resolver.process(new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of("resource_id", resourceId)));

    // mismatched monitor_id
    resolver.process(new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup("not" + metricGroup)
        .setLabels(Map.of(
            "resource_id", "not"+resourceId,
            "monitor_id", monitorId
        )));

    verifyNoInteractions(eventProcessorAdapter);
  }

  @Test
  public void testProcess_justGroupBy() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId1 = randomAlphanumeric(10);
    final String resourceId2 = randomAlphanumeric(10);
    final String monitorId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            .setGroupBy(List.of("monitor_id", "resource_id"))
        );

    resolver.registerTask(task);

    final GroupedMetric metric1 = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(
            "resource_id", resourceId1,
            "monitor_id", monitorId
        ));
    final GroupedMetric metric2 = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(
            "resource_id", resourceId2,
            "monitor_id", monitorId
        ));

    resolver.process(metric1);

    verify(eventProcessorAdapter).process(
        eventProcessorContextArg.capture(),
        eq(List.of(Map.entry("monitor_id", monitorId), Map.entry("resource_id", resourceId1))),
        eq(appProperties.getLocalZone()),
        eq(metric1)
    );

    resolver.process(metric2);

    verify(eventProcessorAdapter).process(
        eventProcessorContextArg.capture(),
        eq(List.of(Map.entry("monitor_id", monitorId), Map.entry("resource_id", resourceId2))),
        eq(appProperties.getLocalZone()),
        eq(metric2)
    );

    // and make sure two distinct contexts were created
    assertThat(eventProcessorContextArg.getAllValues().get(0))
        .isNotEqualTo(eventProcessorContextArg.getAllValues().get(1));

    assertThat(resolver.getContextsForTask(task))
        .hasSize(2);
  }

  /**
   * This test is very similar to {@link #testProcess_justGroupBy()} except the monitor_id
   * is used as a label selector. Otherwise, the context resolution becomes the same result.
   */
  @Test
  public void testProcess_labelSelector_and_groupBy() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId1 = randomAlphanumeric(10);
    final String resourceId2 = randomAlphanumeric(10);
    final String monitorId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            .setLabelSelector(Map.of("monitor_id", monitorId))
            .setGroupBy(List.of("resource_id"))
        );

    resolver.registerTask(task);

    final GroupedMetric metric1 = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(
            "resource_id", resourceId1,
            "monitor_id", monitorId
        ));
    final GroupedMetric metric2 = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:31.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of(
            "resource_id", resourceId2,
            "monitor_id", monitorId
        ));

    resolver.process(metric1);

    verify(eventProcessorAdapter).process(
        eventProcessorContextArg.capture(),
        eq(List.of(Map.entry("monitor_id", monitorId), Map.entry("resource_id", resourceId1))),
        eq(appProperties.getLocalZone()),
        eq(metric1)
    );

    resolver.process(metric2);

    verify(eventProcessorAdapter).process(
        eventProcessorContextArg.capture(),
        eq(List.of(Map.entry("monitor_id", monitorId), Map.entry("resource_id", resourceId2))),
        eq(appProperties.getLocalZone()),
        eq(metric2)
    );

    // and make sure two distinct contexts were created
    assertThat(eventProcessorContextArg.getAllValues().get(0))
        .isNotEqualTo(eventProcessorContextArg.getAllValues().get(1));

    assertThat(resolver.getContextsForTask(task))
        .hasSize(2);
  }

  @Test
  public void testProcess_multipleTasksSameKey() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task1 = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
        );
    resolver.registerTask(task1);

    final EventEngineTask task2 = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
        );
    resolver.registerTask(task2);

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        .setLabels(Map.of("resource_id", resourceId));

    resolver.process(metric);

    verify(eventProcessorAdapter, times(2)).process(
        eventProcessorContextArg.capture(),
        eq(List.of()),
        eq(appProperties.getLocalZone()),
        eq(metric)
    );

    // and make sure two distinct contexts were created and passed one for each task
    assertThat(eventProcessorContextArg.getAllValues().get(0))
        .isNotEqualTo(eventProcessorContextArg.getAllValues().get(1));

    assertThat(resolver.getContextsForTask(task1))
        .hasSize(1);
    assertThat(resolver.getContextsForTask(task2))
        .hasSize(1);
  }

  @Test
  public void testProcess_missingZoneInMetric() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String resourceId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            // with zone label set ...
            .setZoneLabel("monitoring_zone_id")
        );

    resolver.registerTask(task);

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        // ...but no corresponding label
        .setLabels(Map.of());

    resolver.process(metric);

    verify(eventProcessorAdapter).process(
        argThat(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
          return true;
        }),
        eq(List.of()),
        // ...should be special value for unknown zone this time
        eq(appProperties.getUnknownZone()),
        eq(metric)
    );
  }

  @Test
  public void testProcess_remoteZone() {
    final String tenantId = randomAlphanumeric(10);
    final String metricGroup = randomAlphanumeric(10);
    final String zoneId = randomAlphanumeric(10);

    final EventContextResolver resolver = new EventContextResolver(meterRegistry,
        eventProcessorAdapter, appProperties
    );

    final EventEngineTask task = new SalusEventEngineTask()
        .setId(UUID.randomUUID())
        .setTenantId(tenantId)
        .setTaskParameters(new EventEngineTaskParameters()
            .setMetricGroup(metricGroup)
            // with zone label set ...
            .setZoneLabel("monitoring_zone_id")
        );

    resolver.registerTask(task);

    final GroupedMetric metric = new GroupedMetric()
        .setTenantId(tenantId)
        .setTimestamp(Instant.parse("2007-12-03T10:15:30.00Z"))
        .setMetricGroup(metricGroup)
        // ... and label for it
        .setLabels(Map.of("monitoring_zone_id", zoneId));

    resolver.process(metric);

    verify(eventProcessorAdapter).process(
        argThat(eventProcessorContext -> {
          assertThat(eventProcessorContext.getTask()).isSameAs(task);
          assertThat(eventProcessorContext.getStateMachine()).isNotNull();
          return true;
        }),
        eq(List.of()),
        // ...should be the one passed in metric
        eq(zoneId),
        eq(metric)
    );
  }
}