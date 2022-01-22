package se.ivankrizsan.java.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode;
import com.hivemq.client.mqtt.mqtt5.reactor.Mqtt5ReactorClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Abstract base class containing common code used in MQTT-related tests.
 * Can be configured to start an MQTT broker prior to tests being executed and stop
 * it after all the tests in the test-class has been executed.
 *
 * @author Ivan Krizsan
 */
public abstract class MqttTestsAbstractBase {
    /* Constant(s): */
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttTestsAbstractBase.class);
    public static final String TOPIC = "test_mqtt_topic";
    /** Flag indicating whether to use an external MQTT broker or start one using Testcontainers. */
    public static final boolean RUN_MQTT_BROKER_IN_TESTCONTAINER = true;
    public static final String MOSQUITTO_BROKER_DOCKER_IMAGE = "eclipse-mosquitto:2.0.14";
    /** Mosquitto broker configuration used in test broker. */
    public static final String MOSQUITTO_BROKER_CONFIG = "src/test/resources/mosquitto.conf";
    /** Mosquitto broker configuration path in container. */
    protected static final String MOSQUITTO_BROKER_CONFIG_CONTAINER_PATH =
        "/mosquitto/config/mosquitto.conf";
    /** Mosquitto broker password file used in test broker. */
    protected static final String MOSQUITTO_BROKER_PASSWORDFILE =
        "src/test/resources/mosquitto_passwordfile";
    /** Mosquitto broker password file path in container. */
    protected static final String MOSQUITTO_BROKER_PASSWORDFILE_CONTAINER_PATH =
        "/mosquitto/config/mosquitto_passwordfile";
    /** User present in Mosquitto broker password file. */
    protected static final String MOSQUITTO_USERNAME = "testuser";
    /** Password of user present in Mosquitto broker password file. */
    protected static final String MOSQUITTO_PASSWORD = "secret";
    protected static final long MAX_RECONNECT_DELAY_MILLISEC = 10000;
    /** External MQTT broker host. */
    public static final String EXTERNAL_MQTT_BROKER_HOST = "localhost";
    /** External MQTT broker port. */
    public static final int EXTERNAL_MQTT_BROKER_PORT = 1883;
    public static final int DISCONNECT_MQTT_CLIENT_TIMEOUT_MILLISEC = 1000;

    /* Class variable(s): */
    /** Testcontainers container in which the Mosquitto broker used during the tests is running. */
    protected static GenericContainer sMqttBrokerContainer;

    /* Instance variable(s): */
    protected String mMqttBrokerHost;
    protected Integer mMqttBrokerPort;
    protected Mqtt5ReactorClient mMqttPublisher;
    protected String mPublisherId;
    protected Mqtt5ReactorClient mMqttSubscriber;
    protected String mSubscriberId;

    /**
     * Starts a Mosquitto MQTT broker in a container if the test is configured to
     * use a Testcontainers MQTT broker.
     * The MQTT broker will be started once prior to the execution of all tests.
     */
    @BeforeAll
    public static void startMqttBrokerTestContainer() {
        if (RUN_MQTT_BROKER_IN_TESTCONTAINER) {
            sMqttBrokerContainer =
                new GenericContainer<>(MOSQUITTO_BROKER_DOCKER_IMAGE)
                    .withExposedPorts(1883, 9001)
                    .withFileSystemBind(
                        MOSQUITTO_BROKER_CONFIG,
                        MOSQUITTO_BROKER_CONFIG_CONTAINER_PATH)
                    .withFileSystemBind(
                        MOSQUITTO_BROKER_PASSWORDFILE,
                        MOSQUITTO_BROKER_PASSWORDFILE_CONTAINER_PATH)
                    .waitingFor(Wait.forLogMessage(".*mosquitto.*", 1));

            sMqttBrokerContainer.start();
            LOGGER.info("Mosquitto broker started");

            /*
             * Add a log consumer to the Testcontainers container as to have the logs
             * from the Mosquitto container output to the test's logger.
             */
            final Slf4jLogConsumer theMosquittoBrokerLogConsumer =
                new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams();
            sMqttBrokerContainer.followOutput(theMosquittoBrokerLogConsumer);
        }
    }

    /**
     * Stops the Mosquitto MQTT broker in a container if the test after all tests
     * if the broker has been run in a Testcontainers container.
     */
    @AfterAll
    public static void stopMqttBrokerTestContainer() {
        if (RUN_MQTT_BROKER_IN_TESTCONTAINER) {
            sMqttBrokerContainer.stop();
        }
    }

    /**
     * Performs set-up before each test by creating two MQTT clients that will connect
     * to the test MQTT broker.
     */
    @BeforeEach
    public void setUpBeforeTest() {
        if (RUN_MQTT_BROKER_IN_TESTCONTAINER) {
            mMqttBrokerHost = sMqttBrokerContainer.getHost();
            mMqttBrokerPort = sMqttBrokerContainer.getFirstMappedPort();
        } else {
            /* If using an externally started MQTT broker, assume that it is running on localhost:1883. */
            mMqttBrokerHost = EXTERNAL_MQTT_BROKER_HOST;
            mMqttBrokerPort = EXTERNAL_MQTT_BROKER_PORT;
        }
        LOGGER.info("Using MQTT broker running on host '{}' and port {}",
            mMqttBrokerHost,
            mMqttBrokerPort);

        /*
         * Create authentication object holding username and password used
         * by publisher and subscriber.
         */
        final Mqtt5SimpleAuth theMqttAuth = Mqtt5SimpleAuth.builder()
            .username(MOSQUITTO_USERNAME)
            .password(MOSQUITTO_PASSWORD.getBytes(StandardCharsets.UTF_8))
            .build();

        /* Create an MQTT client used for publishing messages. */
        mPublisherId = UUID.randomUUID().toString();
        final Mqtt5Client theRxMqttPublisherClient = MqttClient.builder()
            .useMqttVersion5()
            .identifier(mPublisherId)
            .serverHost(mMqttBrokerHost)
            .serverPort(mMqttBrokerPort)
            .simpleAuth(theMqttAuth)
            .automaticReconnect()
            .maxDelay(MAX_RECONNECT_DELAY_MILLISEC, TimeUnit.MILLISECONDS)
            .applyAutomaticReconnect()
            .buildRx();
        mMqttPublisher = Mqtt5ReactorClient.from(theRxMqttPublisherClient);

        /* Create an MQTT client used for subscribing. */
        mSubscriberId = UUID.randomUUID().toString();
        final Mqtt5Client theRxMqttSubscriberClient = MqttClient.builder()
            .useMqttVersion5()
            .identifier(mSubscriberId)
            .serverHost(mMqttBrokerHost)
            .serverPort(mMqttBrokerPort)
            .simpleAuth(theMqttAuth)
            .automaticReconnect()
            .maxDelay(MAX_RECONNECT_DELAY_MILLISEC, TimeUnit.MILLISECONDS)
            .applyAutomaticReconnect()
            .buildRx();
        mMqttSubscriber = Mqtt5ReactorClient.from(theRxMqttSubscriberClient);
    }

    /**
     * Performs clean-up after each test by disconnecting the MQTT clients.
     */
    @AfterEach
    public void disconnectMqttClients() {
        if (mMqttSubscriber.getState().isConnected()) {
            mMqttSubscriber
                .disconnectWith()
                .reasonCode(Mqtt5DisconnectReasonCode.NORMAL_DISCONNECTION)
                .applyDisconnect()
                .block(Duration.ofMillis(DISCONNECT_MQTT_CLIENT_TIMEOUT_MILLISEC));
        }
        if (mMqttPublisher.getState().isConnected()) {
            mMqttPublisher
                .disconnectWith()
                .reasonCode(Mqtt5DisconnectReasonCode.NORMAL_DISCONNECTION)
                .applyDisconnect()
                .block(Duration.ofMillis(DISCONNECT_MQTT_CLIENT_TIMEOUT_MILLISEC));
        }
    }

    /**
     * Connects the publisher and logs any acknowledge received as result.
     */
    protected void connectPublisher() {
        final Mqtt5ConnAck theConnectionAck = mMqttPublisher
            .connectWith()
            .applyConnect()
            .block(Duration.ofMillis(1000));

        if (theConnectionAck != null) {
            final Mqtt5ConnAckReasonCode theConnectionAckReasonCode =
                theConnectionAck.getReasonCode();
            LOGGER.info("Connecting publisher received ACK code: {}", theConnectionAckReasonCode);
        }
    }

    /**
     * Connects the subscriber and logs any acknowledge received as result.
     */
    protected void connectSubscriber() {
        final Mqtt5ConnAck theConnectionAck = mMqttSubscriber
            .connectWith()
            .applyConnect()
            .block(Duration.ofMillis(1000));

        if (theConnectionAck != null) {
            final Mqtt5ConnAckReasonCode theConnectionAckReasonCode =
                theConnectionAck.getReasonCode();
            LOGGER.info("Connecting subscriber received ACK code: {}", theConnectionAckReasonCode);
        }
    }

    /**
     * Runs the supplied runnable in a new thread.
     *
     * @param inRunnable Runnable to run in a separate thread.
     * @return New thread.
     */
    protected Thread runInNewThread(final Runnable inRunnable) {
        final Thread theThread = new Thread(inRunnable);
        theThread.start();
        return theThread;
    }
}
