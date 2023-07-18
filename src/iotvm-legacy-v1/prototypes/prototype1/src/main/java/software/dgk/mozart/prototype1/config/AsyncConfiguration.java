package software.dgk.mozart.prototype1.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Configuration for asynchronous tasks.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 5 February 2022
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfiguration {}
