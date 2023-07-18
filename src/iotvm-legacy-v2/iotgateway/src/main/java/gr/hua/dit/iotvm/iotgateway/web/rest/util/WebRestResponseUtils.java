package gr.hua.dit.iotvm.iotgateway.web.rest.util;

import java.util.Optional;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ResponseStatusException;

/**
 * Web REST Response utilities and helpers.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class WebRestResponseUtils {

    /* ---------------- Constructors -------------- */

    private WebRestResponseUtils() {}

    /* ---------------- Utilities -------------- */

    /**
     * Wrap the optional into a {@link ResponseEntity} with an {@link HttpStatus#OK} status, or if
     * it's empty, it returns a {@link ResponseEntity} with {@link HttpStatus#NOT_FOUND}.
     *
     * @param <X> type of the response
     * @param maybeResponse response to return if present
     * @return response containing {@code maybeResponse} if present or {@link HttpStatus#NOT_FOUND}
     */
    public static <X> ResponseEntity<X> wrapOrNotFound(Optional<X> maybeResponse) {
        return wrapOrNotFound(maybeResponse, null);
    }

    /**
     * Wrap the optional into a {@link ResponseEntity} with an {@link HttpStatus#OK} status with the
     * headers, or if it's empty, throws a {@link ResponseStatusException} with status {@link
     * HttpStatus#NOT_FOUND}.
     *
     * @param <X> type of the response
     * @param maybeResponse response to return if present
     * @param header headers to be added to the response
     * @return response containing {@code maybeResponse} if present
     */
    public static <X> ResponseEntity<X> wrapOrNotFound(Optional<X> maybeResponse, HttpHeaders header) {
        return maybeResponse
                .map(response -> ResponseEntity.ok().headers(header).body(response))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }
}
