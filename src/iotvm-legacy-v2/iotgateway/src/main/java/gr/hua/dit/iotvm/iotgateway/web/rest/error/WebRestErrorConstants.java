package gr.hua.dit.iotvm.iotgateway.web.rest.error;

import java.net.URI;

/**
 * Constants for the web REST layer.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class WebRestErrorConstants {

    public static final String ERR_CONCURRENCY_FAILURE = "error.concurrencyFailure";
    public static final String ERR_VALIDATION = "error.validation";
    public static final String PROBLEM_BASE_URL = "https://developers.cloutlayer.com/problem";
    public static final URI DEFAULT_TYPE = URI.create(PROBLEM_BASE_URL + "/problem-with-message");
    public static final URI CONSTRAINT_VIOLATION_TYPE = URI.create(PROBLEM_BASE_URL + "/constraint-violation");
    public static final URI BUSINESS_LOGIC_VIOLATION = URI.create(PROBLEM_BASE_URL + "/business-logic-violation");

    private WebRestErrorConstants() {}
}
