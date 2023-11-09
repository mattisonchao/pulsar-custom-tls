package io.github.mattisonchao.pulsar.custom.tls;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class PulsarProxyContainer extends GenericContainer<PulsarProxyContainer> {

    public static final int PROXY_PORT = 6650;
    public static final int PROXY_SSL_PORT = 6651;
    public static final int PROXY_HTTP_PORT = 8080;
    public static final int PROXY_HTTPS_PORT = 8081;

    private static final String STATUS_ENDPOINT = "/status.html";


    private final WaitAllStrategy waitAllStrategy = new WaitAllStrategy();

    public PulsarProxyContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        withExposedPorts(PROXY_PORT, PROXY_SSL_PORT, PROXY_HTTP_PORT, PROXY_HTTPS_PORT);
        setWaitStrategy(waitAllStrategy);
    }

    @Override
    protected void configure() {
        super.configure();
        setupCommandAndEnv();
    }

    @Override
    public String getContainerName() {
        return "proxy";
    }

    public String getProxyBrokerUrl() {
        return String.format("pulsar://%s:%s", getHost(), getMappedPort(PROXY_PORT));
    }

    public String getProxyBrokerSSLUrl(int port) {
        return String.format("pulsar+ssl://%s:%s", getHost(), getMappedPort(port));
    }

    public String getProxyHttpServiceUrl() {
        return String.format("http://%s:%s", getHost(), getMappedPort(PROXY_HTTP_PORT));
    }

    public String getProxyHttpsServiceUrl(int port) {
        return String.format("https://%s:%s", getHost(), getMappedPort(port));
    }

    protected void setupCommandAndEnv() {
        String standaloneBaseCommand =
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/proxy.conf && bin/pulsar proxy";

        withCommand("/bin/bash", "-c", standaloneBaseCommand);

        waitAllStrategy.withStrategy(Wait.forLogMessage(".*Server started.*", 1))
                .withStartupTimeout(Duration.ofMinutes(10));
    }
}
