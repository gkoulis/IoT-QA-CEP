package gr.hua.dit.iotvm.iotgateway.web.rest.error;

import gr.hua.dit.iotvm.iotgateway.web.rest.util.WebRestHeaderUtils;
import jakarta.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.core.env.Environment;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConversionException;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.DefaultProblem;
import org.zalando.problem.Problem;
import org.zalando.problem.ProblemBuilder;
import org.zalando.problem.Status;
import org.zalando.problem.StatusType;
import org.zalando.problem.spring.web.advice.ProblemHandling;
import org.zalando.problem.spring.web.advice.security.SecurityAdviceTrait;
import org.zalando.problem.violations.ConstraintViolationProblem;

/**
 * Controller advice to translate the server side exceptions to client-friendly json structures. The
 * error response follows RFC7807 - Problem Details for HTTP APIs
 * (https://tools.ietf.org/html/rfc7807).
 *
 * <p>The instance of this class ({@link @ControllerAdvice}) must be annotated with {@link
 * ControllerAdvice} annotation.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
@SuppressWarnings("SpringJavaConstructorAutowiringInspection")
public class WebRestExceptionTranslator implements ProblemHandling, SecurityAdviceTrait {

    private static final String SPRING_PROFILE_PRODUCTION = "prod";

    private static final List<String> PACKAGES = new ArrayList<>();
    private static final String EMPTY = "";

    private static final String FIELD_ERRORS_KEY = "fieldErrors";
    private static final String MESSAGE_KEY = "message";
    private static final String PATH_KEY = "path";
    private static final String VIOLATIONS_KEY = "violations";

    static {
        PACKAGES.add("org.");
        PACKAGES.add("java.");
        PACKAGES.add("net.");
        PACKAGES.add("com.");
        PACKAGES.add("io.");
        PACKAGES.add("de.");
        PACKAGES.add("sun.");
        PACKAGES.add("javax.");
        PACKAGES.add("com.cloutlayer.");
    }

    private final String applicationName;
    private final Environment env;

    /* ---------------- Constructors -------------- */

    public WebRestExceptionTranslator(Environment env) {
        this.applicationName = "Cloutlayer";
        this.env = env;
    }

    public WebRestExceptionTranslator(String applicationName, Environment env) {
        this.applicationName = applicationName;
        this.env = env;
    }

    /* ---------------- Helpers -------------- */

    /**
     * Checks if message contains at least one registered package root.
     *
     * <p><strong>Notice</strong> This list is for sure not complete.
     */
    private boolean containsPackageName(String message) {
        if (message == null) {
            return false;
        }
        if (message.isBlank()) {
            return false;
        }
        return PACKAGES.stream().anyMatch(message::contains);
    }

    private static boolean isNotBlank(String message) {
        if (message == null) {
            return false;
        }
        return !message.isBlank();
    }

    /* ---------------- Overrides -------------- */

    /** Post-process the Problem payload to add the message key for the front-end if needed. */
    @Override
    public ResponseEntity<Problem> process(
            @Nullable ResponseEntity<Problem> entity, @Nonnull NativeWebRequest request) {
        if (entity == null) {
            return null;
        }

        Problem problem = entity.getBody();
        if (!(problem instanceof ConstraintViolationProblem || problem instanceof DefaultProblem)) {
            return entity;
        }

        HttpServletRequest nativeRequest = request.getNativeRequest(HttpServletRequest.class);
        String requestUri = nativeRequest != null ? nativeRequest.getRequestURI() : EMPTY;
        ProblemBuilder builder = Problem.builder()
                .withType(
                        Problem.DEFAULT_TYPE.equals(problem.getType())
                                ? WebRestErrorConstants.DEFAULT_TYPE
                                : problem.getType())
                .withStatus(problem.getStatus())
                .withTitle(problem.getTitle())
                .with(PATH_KEY, requestUri);

        if (problem instanceof ConstraintViolationProblem) {
            builder.with(VIOLATIONS_KEY, ((ConstraintViolationProblem) problem).getViolations())
                    .with(MESSAGE_KEY, WebRestErrorConstants.ERR_VALIDATION);
        } else {
            builder.withCause(((DefaultProblem) problem).getCause())
                    .withDetail(problem.getDetail())
                    .withInstance(problem.getInstance());
            problem.getParameters().forEach(builder::with);
            if (!problem.getParameters().containsKey(MESSAGE_KEY) && problem.getStatus() != null) {
                builder.with(MESSAGE_KEY, "error.http." + problem.getStatus().getStatusCode());
            }
        }

        return new ResponseEntity<>(builder.build(), entity.getHeaders(), entity.getStatusCode());
    }

    @Override
    public ResponseEntity<Problem> handleMethodArgumentNotValid(
            MethodArgumentNotValidException ex, @Nonnull NativeWebRequest request) {
        BindingResult result = ex.getBindingResult();
        List<WebRestFieldErrorVM> fieldErrors = result.getFieldErrors().stream()
                .map(f -> new WebRestFieldErrorVM(
                        f.getObjectName().replaceFirst("DTO$", ""),
                        f.getField(),
                        isNotBlank(f.getDefaultMessage()) ? f.getDefaultMessage() : f.getCode()))
                .collect(Collectors.toList());

        Problem problem = Problem.builder()
                .withType(WebRestErrorConstants.CONSTRAINT_VIOLATION_TYPE)
                .withTitle("Method argument not valid")
                .withStatus(defaultConstraintViolationStatus())
                .with(MESSAGE_KEY, WebRestErrorConstants.ERR_VALIDATION)
                .with(FIELD_ERRORS_KEY, fieldErrors)
                .build();
        return create(ex, problem, request);
    }

    @Override
    public ProblemBuilder prepare(
            @Nonnull final Throwable throwable, @Nonnull final StatusType status, @Nonnull final URI type) {
        Collection<String> activeProfiles = Arrays.asList(env.getActiveProfiles());

        if (activeProfiles.contains(SPRING_PROFILE_PRODUCTION)) {
            if (throwable instanceof HttpMessageConversionException) {
                return Problem.builder()
                        .withType(type)
                        .withTitle(status.getReasonPhrase())
                        .withStatus(status)
                        .withDetail("Unable to convert http message")
                        .withCause(Optional.ofNullable(throwable.getCause())
                                .filter(cause -> isCausalChainsEnabled())
                                .map(this::toProblem)
                                .orElse(null));
            }
            if (throwable instanceof DataAccessException) {
                return Problem.builder()
                        .withType(type)
                        .withTitle(status.getReasonPhrase())
                        .withStatus(status)
                        .withDetail("Failure during data access")
                        .withCause(Optional.ofNullable(throwable.getCause())
                                .filter(cause -> isCausalChainsEnabled())
                                .map(this::toProblem)
                                .orElse(null));
            }
            if (containsPackageName(throwable.getMessage())) {
                return Problem.builder()
                        .withType(type)
                        .withTitle(status.getReasonPhrase())
                        .withStatus(status)
                        .withDetail("Unexpected runtime exception")
                        .withCause(Optional.ofNullable(throwable.getCause())
                                .filter(cause -> isCausalChainsEnabled())
                                .map(this::toProblem)
                                .orElse(null));
            }
        }

        return Problem.builder()
                .withType(type)
                .withTitle(status.getReasonPhrase())
                .withStatus(status)
                .withDetail(throwable.getMessage())
                .withCause(Optional.ofNullable(throwable.getCause())
                        .filter(cause -> isCausalChainsEnabled())
                        .map(this::toProblem)
                        .orElse(null));
    }

    /* ---------------- Exception Handlers -------------- */

    @ExceptionHandler
    public ResponseEntity<Problem> handleBadRequestAlertException(
            BadRequestAlertException ex, NativeWebRequest request) {
        return create(
                ex,
                request,
                WebRestHeaderUtils.createFailureAlert(
                        applicationName, true, ex.getEntityName(), ex.getErrorKey(), ex.getMessage()));
    }

    @ExceptionHandler
    public ResponseEntity<Problem> handleConcurrencyFailure(ConcurrencyFailureException ex, NativeWebRequest request) {
        Problem problem = Problem.builder()
                .withStatus(Status.CONFLICT)
                .with(MESSAGE_KEY, WebRestErrorConstants.ERR_CONCURRENCY_FAILURE)
                .build();
        return create(ex, problem, request);
    }
}
