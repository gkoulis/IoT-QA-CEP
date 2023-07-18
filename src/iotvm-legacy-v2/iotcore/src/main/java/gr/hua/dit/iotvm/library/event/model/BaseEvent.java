package gr.hua.dit.iotvm.library.event.model;

import java.io.Serial;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;

public class BaseEvent implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private Double qPropAccuracy;
    private Double qPropCompleteness;
    private Double qPropTimeliness;
    private Double qPropInclusion;
    private EventOriginType originType;
    private Instant eventTimestamp;
    private Instant ingestionTimestamp;
    private Instant processingTimestamp;

    /**
     * Private constructor for Jackson.
     */
    public BaseEvent() {
    }

    public Double getqPropAccuracy() {
        return qPropAccuracy;
    }

    public void setqPropAccuracy(Double qPropAccuracy) {
        this.qPropAccuracy = qPropAccuracy;
    }

    public Double getqPropCompleteness() {
        return qPropCompleteness;
    }

    public void setqPropCompleteness(Double qPropCompleteness) {
        this.qPropCompleteness = qPropCompleteness;
    }

    public Double getqPropTimeliness() {
        return qPropTimeliness;
    }

    public void setqPropTimeliness(Double qPropTimeliness) {
        this.qPropTimeliness = qPropTimeliness;
    }

    public Double getqPropInclusion() {
        return qPropInclusion;
    }

    public void setqPropInclusion(Double qPropInclusion) {
        this.qPropInclusion = qPropInclusion;
    }

    public EventOriginType getOriginType() {
        return originType;
    }

    public void setOriginType(EventOriginType originType) {
        this.originType = originType;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(Instant eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }

    public Instant getIngestionTimestamp() {
        return ingestionTimestamp;
    }

    public void setIngestionTimestamp(Instant ingestionTimestamp) {
        this.ingestionTimestamp = ingestionTimestamp;
    }

    public Instant getProcessingTimestamp() {
        return processingTimestamp;
    }

    public void setProcessingTimestamp(Instant processingTimestamp) {
        this.processingTimestamp = processingTimestamp;
    }

    /* ------------ Logic ------------ */

    public double ageBasedOnEventTimestamp() {
        return Duration.between(this.eventTimestamp, Instant.now()).toMillis();
    }
}
