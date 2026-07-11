package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.response;

import java.math.BigDecimal;
import java.time.Instant;

public record MovementResponse(
        String id,
        String customerId,
        String productId,
        String productType,
        String type,
        BigDecimal amount,
        BigDecimal balanceAfter,
        String transactionId,
        String description,
        String sourceService,
        Instant createdAt
) {
}