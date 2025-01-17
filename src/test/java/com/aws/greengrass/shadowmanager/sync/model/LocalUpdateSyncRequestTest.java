/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.shadowmanager.ShadowManagerDAO;
import com.aws.greengrass.shadowmanager.exception.InvalidRequestParametersException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.ipc.DeleteThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.ipc.UpdateThingShadowRequestHandler;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.UpdateThingShadowHandlerResponse;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.sync.IotDataPlaneClientWrapper;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.ConflictError;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;
import software.amazon.awssdk.aws.greengrass.model.ServiceError;
import software.amazon.awssdk.aws.greengrass.model.UnauthorizedError;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowRequest;
import software.amazon.awssdk.aws.greengrass.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static com.aws.greengrass.shadowmanager.TestUtils.SAMPLE_EXCEPTION_MESSAGE;
import static com.aws.greengrass.shadowmanager.TestUtils.SHADOW_NAME;
import static com.aws.greengrass.shadowmanager.TestUtils.THING_NAME;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@ExtendWith({MockitoExtension.class, GGExtension.class})
class LocalUpdateSyncRequestTest {

    private static final byte[] UPDATE_DOCUMENT = "{\"version\": 6, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

    @Mock
    UpdateThingShadowRequestHandler mockUpdateThingShadowRequestHandler;

    @Mock
    private ShadowManagerDAO mockDao;

    @Captor
    private ArgumentCaptor<SyncInformation> syncInformationCaptor;

    SyncContext syncContext;

    @BeforeEach
    void setup() throws IOException {
        syncContext = new SyncContext(mockDao, mockUpdateThingShadowRequestHandler,
                mock(DeleteThingShadowRequestHandler.class), mock(IotDataPlaneClientWrapper.class));
        JsonUtil.loadSchema();
    }

    @Test
    void GIVEN_good_local_update_request_WHEN_execute_THEN_successfully_updates_local_shadow_and_sync_information() throws SkipSyncRequestException, UnknownShadowException {
        lenient().when(mockDao.updateSyncInformation(syncInformationCaptor.capture())).thenReturn(true);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));
        UpdateThingShadowResponse response = new UpdateThingShadowResponse();
        response.setPayload("{\"version\": 1}".getBytes(UTF_8));
        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenReturn(new UpdateThingShadowHandlerResponse(response, UPDATE_DOCUMENT));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        request.execute(syncContext);

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(1)).updateSyncInformation(any());

        assertThat(syncInformationCaptor.getValue(), is(notNullValue()));
        assertThat(syncInformationCaptor.getValue().getLastSyncedDocument(), is(equalTo(UPDATE_DOCUMENT)));
        assertThat(syncInformationCaptor.getValue().getCloudVersion(), is(6L));
        assertThat(syncInformationCaptor.getValue().getCloudUpdateTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getLastSyncTime(), is(greaterThanOrEqualTo(epochSeconds)));
        assertThat(syncInformationCaptor.getValue().getShadowName(), is(SHADOW_NAME));
        assertThat(syncInformationCaptor.getValue().getThingName(), is(THING_NAME));
        assertThat(syncInformationCaptor.getValue().isCloudDeleted(), is(false));
    }

    @Test
    void GIVEN_bad_cloud_update_payload_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, UnrecognizedPropertyException.class);

        final byte[] badCloudPayload = "{\"version\": 6, \"badUpdate\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, badCloudPayload);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getCause(), is(instanceOf(InvalidRequestParametersException.class)));

        verify(mockDao, times(0)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_cloud_update_with_much_higher_version_WHEN_execute_THEN_throw_full_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ConflictError.class);

        final byte[] cloudPayload =  "{\"version\": 10, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}".getBytes();

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, cloudPayload);
        ConflictError thrown = assertThrows(ConflictError.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), Matchers.startsWith("Missed update(s)"));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"version\": 5, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}",
            "{\"version\": 2, \"state\": {\"reported\": {\"name\": \"The Beatles\"}}}"})
    void GIVEN_cloud_update_with_version_equal_or_less_WHEN_execute_THEN_do_nothing(String updateString) {
        final byte[] cloudPayload =  updateString.getBytes();

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, cloudPayload);
        assertDoesNotThrow(() -> request.execute(syncContext));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(0)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_conflict_error_during_local_shadow_update_WHEN_execute_THEN_throw_full_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ConflictError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new ConflictError(SAMPLE_EXCEPTION_MESSAGE));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        ConflictError thrown = assertThrows(ConflictError.class, () -> request.execute(syncContext));
        assertThat(thrown.getMessage(), is(equalTo(SAMPLE_EXCEPTION_MESSAGE)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_shadow_manager_data_exception_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ShadowManagerDataException.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(ShadowManagerDataException.class);

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getCause(), is(instanceOf(ShadowManagerDataException.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_unauthorized_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, UnauthorizedError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new UnauthorizedError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getCause(), is(instanceOf(UnauthorizedError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_invalid_arguments_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new InvalidArgumentsError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getCause(), is(instanceOf(InvalidArgumentsError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_service_error_during_local_shadow_update_WHEN_execute_THEN_throw_skip_sync_request_exception(ExtensionContext context) {
        ignoreExceptionOfType(context, ServiceError.class);

        long epochSeconds = Instant.now().getEpochSecond();
        when(mockDao.getShadowSyncInformation(anyString(), anyString())).thenReturn(Optional.of(SyncInformation.builder()
                .cloudUpdateTime(epochSeconds)
                .thingName(THING_NAME)
                .shadowName(SHADOW_NAME)
                .cloudDeleted(false)
                .lastSyncedDocument(null)
                .cloudVersion(5L)
                .lastSyncTime(epochSeconds)
                .build()));

        when(mockUpdateThingShadowRequestHandler.handleRequest(any(UpdateThingShadowRequest.class), anyString()))
                .thenThrow(new ServiceError(SAMPLE_EXCEPTION_MESSAGE));


        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        SkipSyncRequestException thrown = assertThrows(SkipSyncRequestException.class, () -> request.execute(syncContext));
        assertThat(thrown.getCause(), is(instanceOf(ServiceError.class)));

        verify(mockDao, times(1)).getShadowSyncInformation(anyString(), anyString());
        verify(mockUpdateThingShadowRequestHandler, times(1)).handleRequest(any(), any());
        verify(mockDao, times(0)).updateSyncInformation(any());
    }

    @Test
    void GIVEN_no_change_WHEN_execute_THEN_does_not_update_local_shadow_and_sync_information() throws IOException {
        ShadowDocument shadowDocument = new ShadowDocument(UPDATE_DOCUMENT);
        when(mockDao.getShadowThing(any(), any())).thenReturn(Optional.of(shadowDocument));

        LocalUpdateSyncRequest request = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);

        assertDoesNotThrow(() -> request.execute(syncContext));

        verify(mockDao, never()).updateSyncInformation(any());
        verify(mockUpdateThingShadowRequestHandler, never()).handleRequest(any(), any());
    }

    static Stream<Arguments> currentUpdatePayloads() {
        return Stream.of(arguments(UPDATE_DOCUMENT));
    }

    @ParameterizedTest
    @NullSource
    @MethodSource("currentUpdatePayloads")
    void GIVEN_updated_local_update_request_WHEN_merge_THEN_return_merged_shadow_sync(byte[] updatePayload1) throws IOException {
        LocalUpdateSyncRequest request1 = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, updatePayload1);
        LocalUpdateSyncRequest request2 = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, "{\"version\": 7, \"state\": {\"reported\": {\"name\": \"Pink Floyd\"}}}".getBytes());
        request1.merge(request2);
        assertThat(JsonUtil.getPayloadJson(request1.getUpdateDocument()), is(JsonUtil.getPayloadJson(request2.getUpdateDocument())));
    }

    @ParameterizedTest
    @NullSource
    void GIVEN_no_update_local_update_request_2_WHEN_merge_THEN_return_first_update_shadow_document(byte[] updatePayload) throws IOException {
        LocalUpdateSyncRequest request1 = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, UPDATE_DOCUMENT);
        LocalUpdateSyncRequest request2 = new LocalUpdateSyncRequest(THING_NAME, SHADOW_NAME, updatePayload);
        request1.merge(request2);
        assertThat(JsonUtil.getPayloadJson(request1.getUpdateDocument()), is(JsonUtil.getPayloadJson(request1.getUpdateDocument())));
    }
}
