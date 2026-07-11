package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.persistence;

import org.springframework.stereotype.Component;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;

@Component
public class MovementPersistenceMapper {

    public MovementDocument toDocument(Movement domain) {
        return MovementDocument.builder()
                .id(domain.id())
                .customerId(domain.customerId())
                .productId(domain.productId())
                .productType(domain.productType())
                .type(domain.type())
                .amount(domain.amount())
                .balanceAfter(domain.balanceAfter())
                .transactionId(domain.transactionId())
                .description(domain.description())
                .sourceService(domain.sourceService())
                .createdAt(domain.createdAt())
                .build();
    }

    public Movement toDomain(MovementDocument document) {
        return new Movement(
                document.getId(),
                document.getCustomerId(),
                document.getProductId(),
                document.getProductType(),
                document.getType(),
                document.getAmount(),
                document.getBalanceAfter(),
                document.getTransactionId(),
                document.getDescription(),
                document.getSourceService(),
                document.getCreatedAt()
        );
    }
}