package gr.hua.dit.iotvm.iotgateway.web.rest.error;

import java.io.Serializable;

/**
 * View-model for fields errors.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public class WebRestFieldErrorVM implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String objectName;
    private final String field;
    private final String message;

    public WebRestFieldErrorVM(String dto, String field, String message) {
        this.objectName = dto;
        this.field = field;
        this.message = message;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getField() {
        return field;
    }

    public String getMessage() {
        return message;
    }
}
