package pe.nom.charlygastelo.app.movementservice.domain.model;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record Movement(
        String id,
        String customerId,
        String productId,
        ProductType productType,
        MovementType type,
        BigDecimal amount,
        BigDecimal balanceAfter,
        String transactionId,
        String description,
        String sourceService,
        LocalDateTime createdAt
) { }