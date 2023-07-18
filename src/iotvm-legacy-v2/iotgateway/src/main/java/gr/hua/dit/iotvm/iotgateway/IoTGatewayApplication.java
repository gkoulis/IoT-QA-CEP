package gr.hua.dit.iotvm.iotgateway;

import gr.hua.dit.iotvm.iotgateway.config.Constants;
import io.micrometer.common.util.StringUtils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;

/**
 * <strong>IoTGatewayApplication</strong>.
 *
 * @author Dimitris Gkoulis
 * @createdAt Thursday 12 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@SpringBootApplication
public class IoTGatewayApplication {

    private static final Logger log = LoggerFactory.getLogger(IoTGatewayApplication.class);

    private static final String SPRING_PROFILE_DEFAULT = "spring.profiles.default";

    /**
     * Set a default to use when no profile is configured.
     *
     * @param app the Spring application.
     */
    public static void addDefaultProfile(SpringApplication app) {
        Map<String, Object> defProperties = new HashMap<>();
        /*
         * The default profile to use when no other profiles are defined
         * This cannot be set in the application.yml file.
         * See https://github.com/spring-projects/spring-boot/issues/1219
         */
        defProperties.put(SPRING_PROFILE_DEFAULT, Constants.SPRING_PROFILE_DEVELOPMENT);
        app.setDefaultProperties(defProperties);
    }

    private static void logApplicationStartup(Environment env) {
        String protocol = Optional.ofNullable(env.getProperty("server.ssl.key-store"))
                .map(key -> "https")
                .orElse("http");
        String serverPort = env.getProperty("server.port");
        String contextPath = Optional.ofNullable(env.getProperty("server.servlet.context-path"))
                .filter(StringUtils::isNotBlank)
                .orElse("/");
        String hostAddress = "localhost";
        try {
            hostAddress = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            log.warn("The host name could not be determined, using `localhost` as fallback");
        }
        log.info(
                "\n"
                        + "----------------------------------------------------------\n"
                        + "\tApplication '{}' is running! Access URLs:\n"
                        + "\tLocal: \t\t{}://localhost:{}{}\n"
                        + "\tExternal: \t{}://{}:{}{}\n"
                        + "\tProfile(s): \t{}\n"
                        + "----------------------------------------------------------",
                env.getProperty("spring.application.name"),
                protocol,
                serverPort,
                contextPath,
                protocol,
                hostAddress,
                serverPort,
                contextPath,
                env.getActiveProfiles().length == 0 ? env.getDefaultProfiles() : env.getActiveProfiles());
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(IoTGatewayApplication.class);
        addDefaultProfile(app);
        Environment env = app.run(args).getEnvironment();
        logApplicationStartup(env);
    }
}
