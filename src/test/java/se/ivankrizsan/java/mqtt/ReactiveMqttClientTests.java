package se.ivankrizsan.java.mqtt;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.datatypes.MqttTopic;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.pubrec.Mqtt5PubRec;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalTime;
import java.util.Optional;

/**
 * Tests showing interactions with a MQTT broker using the reactive HiveMQTT client.
 *
 * @author Ivan Krizsan
 */
public class ReactiveMqttClientTests extends MqttTestsAbstractBase {
    /* Constant(s): */
    private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveMqttClientTests.class);
    protected static final int MESSAGE_COUNT = 10;

    /* Instance variable(s): */

    /**
     * Tests sending and receiving a number of messages to a MQTT topic.
     * Expected result:
     * The publisher should be able to successfully publish a number of messages to the topic.
     * The consumer should be able to successfully consume the same number of messages
     * from the topic.
     */
    @Test
    public void sendAndReceiveMessagesSuccessfullyTest() {
        connectPublisher();
        connectSubscriber();

        /* Subscribe to the topic. */
        LOGGER.info("*** Subscribing to topic " + TOPIC + "...");
        final Mono<Mqtt5SubAck> theSubscriptionAckMono = mMqttSubscriber
            .subscribeWith()
            .topicFilter(TOPIC)
            .qos(MqttQos.EXACTLY_ONCE)
            .applySubscribe();

        /* Verify that the subscription should have been accepted with minimum QoS 2. */
        StepVerifier
            .create(theSubscriptionAckMono.log())
            .expectNextMatches(theSubscriptionAck ->
                Mqtt5SubAckReasonCode.GRANTED_QOS_2 == theSubscriptionAck.getReasonCodes().get(0))
            .expectComplete()
            .verify();

        /*
         * Create a message consumer flux that consumes all messages from a topic.
         * Must create a message consumer prior to publishing messages that are to be consumed.
         */
        final Flux<Mqtt5Publish> theMessageConsumer =
            mMqttSubscriber
                .publishes(MqttGlobalPublishFilter.ALL, true)
                .map(theMqtt5Publish -> {
                    logPublish(theMqtt5Publish);
                    return theMqtt5Publish;
                });

        /* Consume messages from the topic. */
        runInNewThread(() -> {
            StepVerifier
                .create(theMessageConsumer)
                .expectNextCount(MESSAGE_COUNT)
                .expectComplete()
                .verify();

        });

        /* Publish messages. */
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            final String thePayloadString = "Time: " + LocalTime.now();
            final Mqtt5Publish thePublishMsg = Mqtt5Publish
                .builder()
                .topic(TOPIC)
                .qos(MqttQos.EXACTLY_ONCE)
                .payload(thePayloadString.getBytes(StandardCharsets.UTF_8))
                .retain(false)
                .build();
            mMqttPublisher
                .publish(theSubscriber -> theSubscriber.onNext(thePublishMsg))
                .map(thePublishResult -> {
                    /* Just log the publishing results. */
                    logMqttQos2Result((Mqtt5PublishResult.Mqtt5Qos2Result) thePublishResult);
                    return thePublishResult;
                })
                .subscribe();
        }
    }

    /**
     * Logs the supplied result of a MQTT 5 publish message with QoS 2.
     *
     * @param inMqtt5PublishQos2Result Publish result to log.
     */
    protected void logMqttQos2Result(final Mqtt5PublishResult.Mqtt5Qos2Result inMqtt5PublishQos2Result) {
        final Mqtt5PubRec thePubRec =
            inMqtt5PublishQos2Result.getPubRec();
        LOGGER.info("*** Publish result received: {}", thePubRec);
    }

    /**
     * Logs the supplied MQTT 5 publish message.
     *
     * @param inMqtt5Publish Publish message to log.
     */
    protected void logPublish(final Mqtt5Publish inMqtt5Publish) {
        final MqttTopic theReceivedMsgTopic = inMqtt5Publish.getTopic();
        final MqttQos theReceivedMsgQos = inMqtt5Publish.getQos();
        final Optional<ByteBuffer> theReceivedMsgPayloadOptional = inMqtt5Publish.getPayload();
        final String thePayloadString = theReceivedMsgPayloadOptional.isEmpty()
                ? "[n/a]"
                : StandardCharsets.UTF_8.decode(theReceivedMsgPayloadOptional.get()).toString();
        LOGGER.info("*** Received a message from the topic {} with QoS {} and payload '{}'",
                theReceivedMsgTopic,
                theReceivedMsgQos.name(),
                thePayloadString);
    }
}
