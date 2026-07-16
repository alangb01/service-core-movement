package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event.mapper;

import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Component;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementCreatedEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementDeletedEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementRecordedEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementRegisterResponseEvent;

@Component
public class MovementEventMapper {

    public MovementCreatedEvent toMovementCreatedEvent(Movement movement) {
        return MovementCreatedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("MOVEMENT_CREATED")
                .setOccurredAt(Instant.now().toString())
                .setVersion("1.0")
                .setSource("movement-service")
                .setMovementId(value(movement.id()))
                .setCustomerId(value(movement.customerId()))
                .setProductId(value(movement.productId()))
                .setProductType(movement.productType().name())
                .setMovementType(movement.type().name())
                .setAmount(movement.amount().doubleValue())
                .setBalanceAfter(movement.balanceAfter().doubleValue())
                .setTransactionId(value(movement.transactionId()))
                .setDescription(value(movement.description()))
                .build();
    }

    public MovementDeletedEvent toMovementDeletedEvent(Movement movement) {
        return MovementDeletedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("MOVEMENT_DELETED")
                .setOccurredAt(Instant.now().toString())
                .setVersion("1.0")
                .setSource("movement-service")
                .setMovementId(value(movement.id()))
                .setProductId(value(movement.productId()))
                .setTransactionId(value(movement.transactionId()))
                .build();
    }

    public MovementRegisterResponseEvent toMovementRegisterResponseEvent(
            String correlationId,
            Movement movement,
            boolean success,
            String message) {

        return MovementRegisterResponseEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType("MOVEMENT_REGISTER_RESPONSE")
                .setOccurredAt(Instant.now().toString())
                .setVersion("1.0")
                .setSource("movement-service")
                .setCorrelationId(value(correlationId))
                .setMovementId(movement == null ? "" : value(movement.id()))
                .setSuccess(success)
                .setMessage(value(message))
                .build();
    }

    private String value(String value) {
        return value == null ? "" : value;
    }

    public MovementRecordedEvent toMovementRecordedEvent(Movement movement, String eventType) {
        return MovementRecordedEvent.newBuilder()
                .setEventId(UUID.randomUUID().toString())
                .setEventType(eventType)
                .setOccurredAt(Instant.now().toString())
                .setVersion("1.0")
                .setSource("movement-service")
                .setTransactionId(value(movement.transactionId()))
                .setProductId(value(movement.productId()))
                .setProductType(movement.productType().name())
                .setAmount(movement.amount().doubleValue())
                .setMovementType(movement.type().name())
                .build();

    }
}