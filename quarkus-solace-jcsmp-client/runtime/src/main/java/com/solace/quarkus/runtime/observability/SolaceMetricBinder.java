package com.solace.quarkus.runtime.observability;

import io.quarkus.runtime.annotations.Recorder;

@Recorder
public class SolaceMetricBinder {

    public void initMetrics() {
        //        var solace = CDI.current().select(MessagingService.class).get();
        //        var registry = CDI.current().select(MeterRegistry.class).get();
        //        Manageable.ApiMetrics metrics = solace.metrics();
        //        for (Manageable.ApiMetrics.Metric metric : Manageable.ApiMetrics.Metric.values()) {
        //            Gauge.builder("solace." + metric.name().toLowerCase().replace("_", "."), () -> metrics.getValue(metric))
        //                    .register(registry);
        //        }
    }

}
