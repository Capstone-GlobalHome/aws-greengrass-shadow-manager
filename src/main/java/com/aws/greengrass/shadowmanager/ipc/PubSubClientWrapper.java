/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.shadowmanager.ipc;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.shadowmanager.ipc.model.PubSubRequest;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import javax.inject.Inject;

import static com.aws.greengrass.shadowmanager.ShadowManager.SERVICE_NAME;
import static com.aws.greengrass.shadowmanager.model.Constants.*;

/**
 * Class to handle PubSub interaction with the PubSub Event Stream Agent.
 */
public class PubSubClientWrapper {
    private static final Logger logger = LogManager.getLogger(PubSubClientWrapper.class);
    private final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent;

    /**
     * Constructor.
     *
     * @param pubSubIPCEventStreamAgent PubSub event stream agent
     */
    @Inject
    public PubSubClientWrapper(PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent) {
        this.pubSubIPCEventStreamAgent = pubSubIPCEventStreamAgent;
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been rejected.
     *
     * @param rejectRequest The request object containing the reject information.
     */
    public void reject(PubSubRequest rejectRequest) {
        handlePubSubMessagePblish(rejectRequest, SHADOW_PUBLISH_REJECTED_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param acceptRequest The request object containing the accepted information.
     */
    public void accept(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_ACCEPTED_TOPIC);
    }


    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the delta
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the delta information.
     */
    public void delta(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_DELTA_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted and the documents
     * information needs to be published.
     *
     * @param acceptRequest The request object containing the documents information.
     */
    public void documents(PubSubRequest acceptRequest) {
        handlePubSubMessagePblish(acceptRequest, SHADOW_PUBLISH_DOCUMENTS_TOPIC);
    }

    /**
     * Publish the message using PubSub agent when a desired operation for a shadow has been accepted.
     *
     * @param pubSubRequest     The request object containing the accepted information.
     * @param shadowTopicFormat The format for the shadow topic on which to publish the message
     */
    private void handlePubSubMessagePblish(PubSubRequest pubSubRequest, String shadowTopicFormat) {
        try {
            this.pubSubIPCEventStreamAgent.publish(getShadowPublishTopic(pubSubRequest, shadowTopicFormat),
                    pubSubRequest.getPayload(), SERVICE_NAME);
            logger.atTrace()
                    .setEventType(pubSubRequest.getPublishOperation().getLogEventType())
                    .kv(LOG_THING_NAME_KEY, pubSubRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, pubSubRequest.getShadowName())
                    .log("Successfully published PubSub message");
        } catch (InvalidArgumentsError e) {
            logger.atError().cause(e)
                    .kv(LOG_THING_NAME_KEY, pubSubRequest.getThingName())
                    .kv(LOG_SHADOW_NAME_KEY, pubSubRequest.getShadowName())
                    .setEventType(pubSubRequest.getPublishOperation().getLogEventType())
                    .log("Unable to publish PubSub message");
        }
    }

    /**
     * Gets the Shadow name topic prefix.
     *
     * @param ipcRequest Object that includes thingName, shadowName, and operation to form the shadow topic prefix
     * @param topic      The shadow publish topic to be added onto the topic prefix and operation
     * @return the full topic prefix for the shadow name for the publish topic.
     */
    private String getShadowPublishTopic(PubSubRequest ipcRequest, String topic) {
        String shadowTopicPrefix = ipcRequest.getShadowTopicPrefix();
        String publishTopicOp = ipcRequest.getPublishOperation().getOp();
        return shadowTopicPrefix + publishTopicOp + topic;
    }

    /**
     * Capstone-MyGlobalHome modification.
     * Gets the RabbitMQ routing key as defined by
     * https://capstoneadvisor.atlassian.net/wiki/spaces/GH/pages/1945403404/In-home+Shadow+Operations+over+RabbitMQ
     *
     * @param ipcRequest Object that includes thingName, shadowName, and operation to form the shadow topic prefix
     * @param topic      The shadow publish topic to be added onto the topic prefix and operation
     * @return the full routing key to publish to
     */
    public static String getRabbitRoutingKey(PubSubRequest ipcRequest, String topic) {
        String shadowTopicPrefix = String.format(NAMED_SHADOW_RABBIT_ROUTING_PREFIX,
                ipcRequest.getThingName(), ipcRequest.getShadowName());
        String publishTopicOp = ipcRequest.getPublishOperation().getOp();
        String shadowTopicSlashes = publishTopicOp + topic;
        String shadowTopicDots = shadowTopicSlashes.replace('/', '.');
        return shadowTopicPrefix + shadowTopicDots;
    }
}
