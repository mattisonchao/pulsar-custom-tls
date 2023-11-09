package io.github.mattisonchao.pulsar.custom.tls;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class CustomAuthenticationProviderTest {

    private final PulsarBrokerContainer broker = new PulsarBrokerContainer(
            DockerImageName.parse("streamnative/pulsar:2.9.2.12"));

    private final PulsarProxyContainer proxy = new PulsarProxyContainer(
            DockerImageName.parse("streamnative/pulsar:2.9.2.12"));

    private static final String CA_PATH = CustomAuthenticationProviderTest.class.getResource("/ca.cert.pem").getPath();
    private static final String CLIENT_CERT_PATH = CustomAuthenticationProviderTest.class.getResource("/client.cert.pem").getPath();
    private static final String CLIENT_KEY_PATH = CustomAuthenticationProviderTest.class.getResource("/client.key-pk8.pem").getPath();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeClass
    public void init() throws JsonProcessingException {
        final String userPath = System.getProperty("user.dir");
        final String customProviderPath = userPath + "/build/libs/pulsar-custom-tls.jar";
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("tlsCertFile", "/certs/broker_client.cert.pem");
        brokerClientAuthParams.put("tlsKeyFile", "/certs/broker_client.key-pk8.pem");
        final String brokerClientAuthParamStr = objectMapper.writeValueAsString(brokerClientAuthParams);
        final Map<String, String> proxyAuthParams = new HashMap<>();
        proxyAuthParams.put("tlsCertFile", "/certs/proxy.cert.pem");
        proxyAuthParams.put("tlsKeyFile", "/certs/proxy.key-pk8.pem");
        final String proxyAuthParamStr = objectMapper.writeValueAsString(proxyAuthParams);
        final Network network = Network.newNetwork();
        broker
                .withNetwork(network)
                .withNetworkAliases("pulsar-broker")
                .withCopyFileToContainer(MountableFile.forHostPath(customProviderPath), "/pulsar/lib/pulsar-custom-tls.jar")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/ca.cert.pem"), "/certs/ca.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/server.cert.pem"), "/certs/server.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/server.key-pk8.pem"), "/certs/server.key-pk8.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/broker_client.cert.pem"), "/certs/broker_client.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/broker_client.key-pk8.pem"), "/certs/broker_client.key-pk8.pem")
                .withEnv("PULSAR_PREFIX_tlsEnabled", "true")
                .withEnv("PULSAR_PREFIX_brokerServicePort", "7750")
                .withEnv("PULSAR_PREFIX_brokerServicePortTls", "7751")
                .withEnv("PULSAR_PREFIX_webServicePort", "9090")
                .withEnv("PULSAR_PREFIX_webServicePortTls", "9091")
                .withEnv("PULSAR_PREFIX_advertisedAddress", "pulsar-broker")
                .withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/certs/ca.cert.pem")
                .withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/certs/server.cert.pem")
                .withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/certs/server.key-pk8.pem")
                .withEnv("PULSAR_PREFIX_authenticationEnabled", "true")
                .withEnv("PULSAR_PREFIX_authenticationProviders", "io.github.mattisonchao.pulsar.custom.tls.CustomAuthenticationProvider")
                //  -------- For standalone, because standalone will use internal client to init namespace
                .withEnv("PULSAR_PREFIX_brokerClientTlsEnabled", "true")
                .withEnv("PULSAR_PREFIX_brokerClientTrustCertsFilePath", "/certs/ca.cert.pem")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationTls")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters", brokerClientAuthParamStr);
        broker.start();
        proxy
                .withNetwork(network)
                .withNetworkAliases("pulsar-proxy")
                .withCopyFileToContainer(MountableFile.forHostPath(customProviderPath), "/pulsar/lib/pulsar-custom-tls.jar")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/ca.cert.pem"), "/certs/ca.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/server.cert.pem"), "/certs/server.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/server.key-pk8.pem"), "/certs/server.key-pk8.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/proxy.cert.pem"), "/certs/proxy.cert.pem")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/proxy.key-pk8.pem"), "/certs/proxy.key-pk8.pem")
                // proxy server
                .withEnv("PULSAR_PREFIX_servicePort", "6650")
                .withEnv("PULSAR_PREFIX_servicePortTls", "6651")
                .withEnv("PULSAR_PREFIX_webServicePort", "8080")
                .withEnv("PULSAR_PREFIX_webServicePortTls", "8081")
                .withEnv("PULSAR_PREFIX_tlsTrustCertsFilePath", "/certs/ca.cert.pem")
                .withEnv("PULSAR_PREFIX_tlsCertificateFilePath", "/certs/server.cert.pem")
                .withEnv("PULSAR_PREFIX_tlsKeyFilePath", "/certs/server.key-pk8.pem")
                .withEnv("PULSAR_PREFIX_authenticationEnabled", "true")
                .withEnv("PULSAR_PREFIX_authenticationProviders", "io.github.mattisonchao.pulsar.custom.tls.CustomAuthenticationProvider")
                // proxy -> broker
                .withEnv("PULSAR_PREFIX_brokerProxyAllowedTargetPorts", "*")
                .withEnv("PULSAR_PREFIX_brokerServiceURL", "pulsar://pulsar-broker:7750")
                .withEnv("PULSAR_PREFIX_brokerServiceURLTLS", "pulsar+ssl://pulsar-broker:7751")
                .withEnv("PULSAR_PREFIX_brokerWebServiceURL", "http://pulsar-broker:9090")
                .withEnv("PULSAR_PREFIX_brokerWebServiceURLTLS", "https://pulsar-broker:9091")
                .withEnv("PULSAR_PREFIX_tlsEnabledWithBroker", "true")
                .withEnv("PULSAR_PREFIX_brokerClientTlsEnabled", "true")
                .withEnv("PULSAR_PREFIX_brokerClientTrustCertsFilePath", "/certs/ca.cert.pem")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationPlugin", "org.apache.pulsar.client.impl.auth.AuthenticationTls")
                .withEnv("PULSAR_PREFIX_brokerClientAuthenticationParameters", proxyAuthParamStr);

        proxy.start();
    }

    @Test
    public void testProxyProduceAndConsume() throws PulsarClientException, JsonProcessingException {
        final Map<String, String> adminAuthParams = new HashMap<>();
        adminAuthParams.put("tlsCertFile", CLIENT_CERT_PATH);
        adminAuthParams.put("tlsKeyFile", CLIENT_KEY_PATH);
        final String adminAuthParamsStr = objectMapper.writeValueAsString(adminAuthParams);
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(proxy.getProxyBrokerSSLUrl(6651))
                .tlsTrustCertsFilePath(CA_PATH)
                .authentication("org.apache.pulsar.client.impl.auth.AuthenticationTls", adminAuthParamsStr)
                .build();

        @Cleanup final Consumer<byte[]> consumer = client.newConsumer()
                .topic("persistent://public/default/test-1")
                .subscriptionName("sub-1")
                .subscribe();
        @Cleanup final Producer<byte[]> producer = client.newProducer()
                .topic("persistent://public/default/test-1")
                .create();

        producer.send("test-message".getBytes(StandardCharsets.UTF_8));
        final Message<byte[]> message = consumer.receive();
        Assert.assertEquals(new String(message.getData(), StandardCharsets.UTF_8), "test-message");
    }

}
