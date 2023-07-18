package gr.hua.dit.iotvm.iotgateway.web.rest.util;

import java.util.ArrayList;
import java.util.List;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

/**
 * Web REST Page utilities and helpers.
 *
 * @author Dimitris Gkoulis
 * @createdAt Saturday 24 January 2023
 * @lastModifiedAt never
 * @since 1.0.0-PROTOTYPE.1
 */
public final class WebRestPageUtils {

    /* ---------------- Constructors -------------- */

    private WebRestPageUtils() {}

    /* ---------------- Utilities -------------- */

    /**
     * Create a {@link Page} from a {@link List} of objects
     *
     * @param list list of objects
     * @param pageable pagination information.
     * @param <T> type of object
     * @return page containing objects, and attributes set according to pageable
     * @throws IllegalArgumentException - if list is null
     */
    public static <T> Page<T> createPageFromList(List<T> list, Pageable pageable) {
        if (list == null) {
            throw new IllegalArgumentException("To create a Page, the list mustn't be null!");
        }

        int startOfPage = pageable.getPageNumber() * pageable.getPageSize();
        if (startOfPage > list.size()) {
            return new PageImpl<>(new ArrayList<>(), pageable, 0);
        }

        int endOfPage = Math.min(startOfPage + pageable.getPageSize(), list.size());
        return new PageImpl<>(list.subList(startOfPage, endOfPage), pageable, list.size());
    }
}
