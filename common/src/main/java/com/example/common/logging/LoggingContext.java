package com.example.common.logging;

import org.slf4j.MDC;

/**
 * Centralizovan helper za MDC polja koja koristimo svuda.
 * Postavljanjem konzistentnih kljuceva, log pretraga radi na isti nacin
 * preko sva tri servisa.
 *
 * Standardni set:
 *   traceId   - automatski postavlja Micrometer Tracing
 *   spanId    - automatski postavlja Micrometer Tracing
 *   orderId   - poslovni ID porudzbine
 *   eventId   - ID konkretnog event-a (za debugging duplikata)
 *   eventType - "OrderCreated", "InventoryReserved", itd.
 *   status    - "CREATED", "CONFIRMED", "REJECTED"
 *
 * Pravilo: pozovi setOrderContext(...) na ulazu u event obradu,
 * pa clear() u finally bloku.
 */
public final class LoggingContext {

    public static final String ORDER_ID = "orderId";
    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String STATUS = "status";

    private LoggingContext() {}

    public static void setOrderContext(String orderId, String eventId, String eventType) {
        if (orderId != null) MDC.put(ORDER_ID, orderId);
        if (eventId != null) MDC.put(EVENT_ID, eventId);
        if (eventType != null) MDC.put(EVENT_TYPE, eventType);
    }

    public static void setStatus(String status) {
        if (status != null) MDC.put(STATUS, status);
    }

    public static void clear() {
        MDC.remove(ORDER_ID);
        MDC.remove(EVENT_ID);
        MDC.remove(EVENT_TYPE);
        MDC.remove(STATUS);
    }
}
