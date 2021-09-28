/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests;

import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.shadowmanager.ShadowManagerDAOImpl;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.SyncHandler;
import com.aws.greengrass.shadowmanager.sync.strategy.PeriodicSyncStrategy;
import com.aws.greengrass.shadowmanager.sync.strategy.RealTimeSyncStrategy;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.iotdataplane.model.GetThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.ResourceNotFoundException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Supplier;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static com.github.grantwest.eventually.EventuallyLambdaMatcher.eventuallyEval;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class SyncDirectionalityTest extends NucleusLaunchUtils {
    public static final String MOCK_THING_NAME_1 = "Thing1";
    public static final String CLASSIC_SHADOW = "";

    private Supplier<Optional<SyncInformation>> syncInfo;
    private Supplier<Optional<ShadowDocument>> localShadow;
    private static final String localShadowContentV1 = "{\"state\":{ \"desired\": { \"SomeKey\": \"foo\"}, "
            + "\"reported\":{\"SomeKey\":\"bar\",\"OtherKey\": 1}}}";

    @Mock
    UpdateThingShadowResponse mockUpdateThingShadowResponse;

    @Captor
    private ArgumentCaptor<UpdateThingShadowRequest> cloudUpdateThingShadowRequestCaptor;
    @Captor
    private ArgumentCaptor<software.amazon.awssdk.services.iotdataplane.model.DeleteThingShadowRequest> cloudDeleteThingShadowRequestCaptor;

    @BeforeEach
    void setup() {
        kernel = new Kernel();
        syncInfo = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowSyncInformation(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
        localShadow = () -> kernel.getContext().get(ShadowManagerDAOImpl.class).getShadowThing(MOCK_THING_NAME_1,
                CLASSIC_SHADOW);
    }

    @AfterEach
    void cleanup() {
        kernel.shutdown();
    }

    private void assertEmptySyncQueue(Class<?> clazz) {
        if (RealTimeSyncStrategy.class.equals(clazz)) {
            assertThat(() -> kernel.getContext().get(RealTimeSyncStrategy.class).getSyncQueue().size(), eventuallyEval(is(0)));
        } else {
            assertThat(() -> kernel.getContext().get(PeriodicSyncStrategy.class).getSyncQueue().size(), eventuallyEval(is(0)));
        }
    }

    @Test
    void ewsaef(ExtensionContext context) throws InterruptedException, IOException {
        ignoreExceptionOfType(context, InterruptedException.class);
        ignoreExceptionOfType(context, InterruptedException.class);

        // no shadow exists in cloud
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().getThingShadow(any(GetThingShadowRequest.class)))
                .thenThrow(ResourceNotFoundException.class);

        // mock response to update cloud
        when(mockUpdateThingShadowResponse.payload()).thenReturn(SdkBytes.fromString("{\"version\": 1}", UTF_8));
        when(iotDataPlaneClientFactory.getIotDataPlaneClient().updateThingShadow(cloudUpdateThingShadowRequestCaptor.capture()))
                .thenReturn(mockUpdateThingShadowResponse);

        startNucleusWithConfig(NucleusLaunchUtilsConfig.builder()
                .configFile("sync_directionality_bidirectional.yaml")
                .mockCloud(true)
                .build());

        UpdateThingShadowRequestHandler updateHandler = shadowManager.getUpdateThingShadowRequestHandler();

        // update local shadow
        software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest request = new software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest();
        request.setThingName(MOCK_THING_NAME_1);
        request.setShadowName(CLASSIC_SHADOW);
        request.setPayload(localShadowContentV1.getBytes(UTF_8));

        updateHandler.handleRequest(request, "DoAll");
        assertThat(() -> kernel.getContext().get(RealTimeSyncStrategy.class).getSyncQueue().size(), eventuallyEval(is(0)));

        assertThat("sync info exists", () -> syncInfo.get().isPresent(), eventuallyEval(is(true)));

        assertThat("cloud version", () -> syncInfo.get().get().getCloudVersion(), eventuallyEval(is(1L)));
        assertThat("local version", syncInfo.get().get().getLocalVersion(), is(1L));

        assertThat("local shadow exists", localShadow.get().isPresent(), is(true));
        ShadowDocument shadowDocument = localShadow.get().get();
        // remove metadata node and version (JsonNode version will fail a comparison of long vs int)
        shadowDocument = new ShadowDocument(shadowDocument.getState(), null, null);
        JsonNode v1 = new ShadowDocument(localShadowContentV1.getBytes(UTF_8)).toJson(false);
        assertThat(shadowDocument.toJson(false), is(v1));

        verify(iotDataPlaneClientFactory.getIotDataPlaneClient(), times(1)).updateThingShadow(
                any(software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowRequest.class));
    }
}
