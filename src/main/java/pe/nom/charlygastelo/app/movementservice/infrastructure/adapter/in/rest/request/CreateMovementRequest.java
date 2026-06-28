package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.request;

import java.math.BigDecimal;

public record CreateMovementRequest(
        String customerId,
        String productId,
        String productType,
        String type,
        BigDecimal amount,
        BigDecimal balanceAfter,
        String transactionId,
        String description,
        String sourceService
) {
}