/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.sync.model;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.exception.RetryableException;
import com.aws.greengrass.shadowmanager.exception.ShadowManagerDataException;
import com.aws.greengrass.shadowmanager.exception.SkipSyncRequestException;
import com.aws.greengrass.shadowmanager.exception.UnknownShadowException;
import com.aws.greengrass.shadowmanager.model.ShadowDocument;
import com.aws.greengrass.shadowmanager.model.dao.SyncInformation;
import com.aws.greengrass.shadowmanager.util.JsonMerger;
import com.aws.greengrass.shadowmanager.util.JsonUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.iotdataplane.model.ConflictException;
import software.amazon.awssdk.services.iotdataplane.model.InternalFailureException;
import software.amazon.awssdk.services.iotdataplane.model.ServiceUnavailableException;
import software.amazon.awssdk.services.iotdataplane.model.ThrottlingException;
import software.amazon.awssdk.services.iotdataplane.model.UpdateThingShadowResponse;

import java.io.IOException;
import java.util.Optional;

import static com.aws.greengrass.shadowmanager.model.Constants.LOG_SHADOW_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.LOG_THING_NAME_KEY;
import static com.aws.greengrass.shadowmanager.model.Constants.SHADOW_DOCUMENT_VERSION;

/**
 * Sync request to update shadow in the cloud.
 */
public class CloudUpdateSyncRequest extends BaseSyncRequest {
    private static final Logger logger = LogManager.getLogger(CloudUpdateSyncRequest.class);

    @NonNull
    JsonNode updateDocument;

    /**
     * Ctr for CloudUpdateSyncRequest.
     *
     * @param thingName      The thing name associated with the sync shadow update
     * @param shadowName     The shadow name associated with the sync shadow update
     * @param updateDocument The update request bytes.
     */
    public CloudUpdateSyncRequest(String thingName,
                                  String shadowName,
                                  JsonNode updateDocument) {
        super(thingName, shadowName);
        this.updateDocument = updateDocument;
    }

    /**
     * Merge the sync requests together.
     * @param other the newer request to merge
     */
    public void merge(CloudUpdateSyncRequest other) {
        JsonMerger.merge(updateDocument, other.updateDocument);
    }

    /**
     * Executes a cloud shadow update after a successful local shadow update.
     *
     * @throws ConflictException        if cloud version is not the same as the version in the cloud.
     * @throws RetryableException       if the cloud is throttling the request or some other transient issue.
     * @throws SkipSyncRequestException if the update request on the cloud shadow failed for another 400 exception.
     * @throws UnknownShadowException   if the shadow sync information is missing
     */
    @SuppressWarnings("PMD.PrematureDeclaration")
    @Override
    public void execute(SyncContext context) throws RetryableException, SkipSyncRequestException,
            ConflictException, UnknownShadowException {
        Optional<ShadowDocument> shadowDocument = context.getDao().getShadowThing(getThingName(), getShadowName());

        if (!shadowDocument.isPresent()) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Unable to sync shadow since shadow does not exist");
            return;
        }

        SyncInformation currentSyncInformation = context.getDao()
                .getShadowSyncInformation(getThingName(), getShadowName())
                .orElseThrow(() -> new UnknownShadowException("Shadow not found in sync table"));

        if (!isUpdateNecessary(currentSyncInformation.getLastSyncedDocument(), updateDocument)) {
            logger.atWarn()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Cloud shadow already contains update payload. No sync is necessary");
            return;
        }

        long cloudVersion = getAndUpdateCloudVersionInRequest(currentSyncInformation);
        UpdateThingShadowResponse response;
        try {
            logger.atDebug()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .log("Updating cloud shadow document");
            response = context.getIotDataPlaneClientWrapper().updateThingShadow(getThingName(), getShadowName(),
                    JsonUtil.getPayloadBytes(updateDocument));
        } catch (ConflictException e) {  // NOPMD - Throw ConflictException instead of treated as SdkServiceException
            throw e;
        } catch (ThrottlingException | ServiceUnavailableException | InternalFailureException e) {
            throw new RetryableException(e);
        } catch (SdkServiceException | SdkClientException | IOException e) {
            throw new SkipSyncRequestException(e);
        }

        try {
            context.getDao().updateSyncInformation(SyncInformation.builder()
                    .lastSyncedDocument(JsonUtil.getPayloadBytes(shadowDocument.get().toJson(false)))
                    .cloudVersion(getUpdatedVersion(response.payload().asByteArray()).orElse(cloudVersion + 1))
                    .cloudDeleted(false)
                    .shadowName(getShadowName())
                    .thingName(getThingName())
                    .cloudUpdateTime(shadowDocument.get().getMetadata().getLatestUpdatedTimestamp())
                    .localVersion(shadowDocument.get().getVersion())
                    .build());
        } catch (JsonProcessingException | ShadowManagerDataException e) {
            logger.atError()
                    .kv(LOG_THING_NAME_KEY, getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, getShadowName())
                    .cause(e).log("Failed to update sync table after updating cloud shadow");
        }
    }

    private long getAndUpdateCloudVersionInRequest(SyncInformation syncInformation) {
        long cloudVersion = syncInformation.getCloudVersion();
        if (cloudVersion != 0) {
            ((ObjectNode) updateDocument).set(SHADOW_DOCUMENT_VERSION, new LongNode(cloudVersion));
        }

        return cloudVersion;
    }
}
