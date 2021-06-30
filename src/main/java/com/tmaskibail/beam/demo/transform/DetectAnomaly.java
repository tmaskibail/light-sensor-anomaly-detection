package com.tmaskibail.beam.demo.transform;

import com.tmaskibail.beam.demo.model.SensorData;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;

public class DetectAnomaly extends DoFn<SensorData, SensorData> {
    private static final Logger LOG = LoggerFactory.getLogger(DetectAnomaly.class);
    private final PCollectionView<Map<String, BigDecimal>> sideInputMap;

    public DetectAnomaly(PCollectionView<Map<String, BigDecimal>> sideInputMap) {
        this.sideInputMap = sideInputMap;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Map<String, BigDecimal> criteriaMap = c.sideInput(sideInputMap);

        // TODO: Fix the mess below!
        if ((c.element().getAmbientLight().compareTo(criteriaMap.get("LOWER_BOUND").doubleValue()) < 0)
                || (c.element().getAmbientLight().compareTo(criteriaMap.get("UPPER_BOUND").doubleValue())) > 0) {
            LOG.info("Anomaly detected with ambient light : {}", c.element());
            c.output(c.element());
        }
    }
}
