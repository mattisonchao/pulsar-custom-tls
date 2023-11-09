package io.github.mattisonchao.pulsar.custom.tls;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.naming.AuthenticationException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.AuthenticationDataSource;
import org.apache.pulsar.broker.authentication.AuthenticationProvider;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomAuthenticationProvider implements AuthenticationProvider {
    private static final Logger log = LoggerFactory.getLogger(CustomAuthenticationProvider.class);
    @Override
    public void initialize(ServiceConfiguration config) {
        // no-op
    }


    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public String getAuthMethodName() {
        return "tls";
    }

    @Override
    public String authenticate(@Nonnull AuthenticationDataSource authData) throws AuthenticationException {
        Objects.requireNonNull(authData);
        String commonName = null;
        try {
            if (authData.hasDataFromTls()) {
                log.info("Test log 1");
                /**
                 * Maybe authentication type should be checked if it is an HTTPS session. However this check fails actually
                 * because authType is null.
                 *
                 * This check is not necessarily needed, because an untrusted certificate is not passed to
                 * HttpServletRequest.
                 *
                 * <code>
                 * if (authData.hasDataFromHttp()) {
                 *     String authType = authData.getHttpAuthType();
                 *     if (!HttpServletRequest.CLIENT_CERT_AUTH.equals(authType)) {
                 *         throw new AuthenticationException(
                 *              String.format( "Authentication type mismatch, Expected: %s, Found: %s",
                 *                       HttpServletRequest.CLIENT_CERT_AUTH, authType));
                 *     }
                 * }
                 * </code>
                 */

                // Extract CommonName
                // The format is defined in RFC 2253.
                // Example:
                // CN=Steve Kille,O=Isode Limited,C=GB
                final Certificate[] certs = authData.getTlsCertificates();
                log.info("Test log 2");
                if (null == certs) {
                    throw new AuthenticationException("Failed to get TLS certificates from client");
                }
                final String distinguishedName = ((X509Certificate) certs[0]).getSubjectX500Principal().getName();
                log.info("Test log 3");
                for (String keyValueStr : distinguishedName.split(",")) {
                    final String[] keyValue = keyValueStr.split("=", 2);
                    if (keyValue.length == 2 && "CN".equals(keyValue[0]) && !keyValue[1].isEmpty()) {
                        commonName = keyValue[1];
                        break;
                    }
                }
            }

            if (commonName == null) {
                log.info("Test log 4");
                throw new AuthenticationException("Client unable to authenticate with TLS certificate");
            }
            log.info("Test log 5");
            AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
        } catch (AuthenticationException exception) {
            AuthenticationMetrics.authenticateFailure(getClass().getSimpleName(), getAuthMethodName(), exception.getMessage());
            throw exception;
        }
        return commonName;
    }
}
