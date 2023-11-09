package io.github.mattisonchao.pulsar.custom.tls;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

public class PulsarBrokerContainer extends GenericContainer<PulsarBrokerContainer> {

    private final WaitAllStrategy waitAllStrategy = new WaitAllStrategy();

    private boolean functionsWorkerEnabled = false;



    public PulsarBrokerContainer(final DockerImageName dockerImageName) {
        super(dockerImageName);
        setWaitStrategy(waitAllStrategy);
    }

    @Override
    protected void configure() {
        super.configure();
        setupCommandAndEnv();
    }

    @Override
    public String getContainerName() {
        return "broker";
    }

    protected void setupCommandAndEnv() {
        String standaloneBaseCommand =
                "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf " + "&& bin/pulsar standalone";

        if (!functionsWorkerEnabled) {
            standaloneBaseCommand += " --no-functions-worker -nss";
        }

        withCommand("/bin/bash", "-c", standaloneBaseCommand);

        waitAllStrategy.withStrategy(Wait.forLogMessage(".*messaging service is ready.*", 1))
                .withStartupTimeout(Duration.ofMinutes(10));
        if (functionsWorkerEnabled) {
            waitAllStrategy.withStrategy(Wait.forLogMessage(".*Function worker service started.*", 1));
        }
    }
}
