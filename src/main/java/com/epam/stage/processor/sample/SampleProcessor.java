/*
 * Copyright 2017 StreamSets Inc.
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
package com.epam.stage.processor.sample;

import ch.hsr.geohash.GeoHash;
import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageLatLng;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.byteowls.jopencage.model.JOpenCageReverseRequest;
import com.epam.stage.lib.sample.Errors;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

public abstract class SampleProcessor extends SingleLaneRecordProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SampleProcessor.class);
    private static final String TOKEN = "d47704e9ddf84a9c931ad8082dab2a22";

    /**
     * Gives access to the UI configuration of the stage provided by the {@link SampleDProcessor} class.
     */
    public abstract String getConfig();

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        if (getConfig().equals("invalidValue")) {
            issues.add(
                    getContext().createConfigIssue(
                            Groups.SAMPLE.name(), "config", Errors.SAMPLE_00, "Here's what's wrong..."
                    )
            );
        }

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return issues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
        LOG.info("Input record: {}", record);
        Field latitude = record.get("/Latitude");
        Field longitude = record.get("/Longitude");

        Field address = record.get("/Address");
        Field city = record.get("/City");
        Field country = record.get("/Country");
        if (longitude.getValueAsString().isEmpty() || "NA".equals(longitude.getValueAsString()) || latitude.getValueAsString().isEmpty() || "NA".equals(latitude.getValueAsString())) {

            JOpenCageGeocoder jOpenCageGeocoder = new JOpenCageGeocoder("d47704e9ddf84a9c931ad8082dab2a22");
            JOpenCageForwardRequest request = new JOpenCageForwardRequest(address.getValueAsString()+", "+city.getValueAsString()+", "+ country.getValueAsString());

            JOpenCageResponse response = jOpenCageGeocoder.forward(request);
            JOpenCageLatLng firstResultLatLng = response.getFirstPosition();
            Double lat = firstResultLatLng.getLat();
            record.set("/Latitude",Field.create(lat.toString()));
            Double lng = firstResultLatLng.getLng();
            record.set("/Longitude",Field.create(lng.toString()));
            String geo = GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5);
            record.set("/Geohash",Field.create(geo));
            LOG.info("GEOHASH: " + geo);
            LOG.info("LONGITUDE: " + firstResultLatLng.getLat().toString());
            LOG.info("LATITUDE: " + firstResultLatLng.getLng().toString());
            batchMaker.addRecord(record);

        }


    }
}
