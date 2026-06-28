package pe.nom.charlygastelo.app.movementservice.infrastructure.events;

import java.math.BigDecimal;
import java.time.LocalDateTime;


import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.application.usecase.CreateMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.MovementType;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.infrastructure.events.mapper.MovementEventMapper;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementRegisterRequestEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementRegisterRequestConsumer {

    private final AvroJsonDeserializer avroJsonDeserializer;
    private final CreateMovementUseCase createMovementUseCase;
    private final MovementRegisterResponseProducer responseProducer;
    private final MovementEventMapper mapper;

    @KafkaListener(topics = "${topic.movement-register-request}", groupId = "movement-service")
    public void consume(String message) {
        try {
            MovementRegisterRequestEvent event =
                    avroJsonDeserializer.deserialize(
                            message,
                            MovementRegisterRequestEvent.class,
                            MovementRegisterRequestEvent.getClassSchema()
                    );

            String correlationId = event.getCorrelationId().toString();

            Movement movement = new Movement(
                    null,
                    event.getCustomerId().toString(),
                    event.getProductId().toString(),
                    ProductType.valueOf(event.getProductType().toString()),
                    MovementType.valueOf(event.getMovementType().toString()),
                    BigDecimal.valueOf(event.getAmount()),
                    BigDecimal.valueOf(event.getBalanceAfter()),
                    event.getTransactionId().toString(),
                    event.getDescription().toString(),
                    event.getSource().toString(),
                    LocalDateTime.now()
            );

            createMovementUseCase.execute(movement)
                    .subscribe(
                            saved -> responseProducer.publish(
                                    correlationId,
                                    mapper.toMovementRegisterResponseEvent(
                                            correlationId,
                                            saved,
                                            true,
                                            "Movement registered successfully"
                                    )
                            ),
                            error -> responseProducer.publish(
                                    correlationId,
                                    mapper.toMovementRegisterResponseEvent(
                                            correlationId,
                                            null,
                                            false,
                                            error.getMessage()
                                    )
                            )
                    );

        }
        catch (Exception e) {
            log.error("Error processing MovementRegisterRequestEvent", e);
        }
    }
}