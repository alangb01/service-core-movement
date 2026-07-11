package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.event;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.application.usecase.CreateMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.MovementType;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event.MovementEventProducer;
import pe.nom.charlygastelo.app.shared.avro.dto.AccountInitialDepositEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementCreateRequestEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementRegisterRequestConsumer {

    private final CreateMovementUseCase createMovementUseCase;
    private final MovementEventProducer movementEventProducer;

    @KafkaListener(topics = "${topic.movement-register-request}", groupId = "movement-service")
    public void consume(MovementCreateRequestEvent event) {
        try {
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
                    Instant.now()
            );

            createMovementUseCase.execute(movement)
                .subscribe(
                    saved -> movementEventProducer.publishMovementCreated(movement)
                );

        }
        catch (Exception e) {
            log.error("Error processing MovementRegisterRequestEvent", e);
        }
    }

    @KafkaListener(topics = "${topic.movement-initial-deposit-request}", groupId = "movement-service")
    public void consume(AccountInitialDepositEvent event) {
        try {
            Movement movement = new Movement(
                    null,
                    event.getCustomerId().toString(),
                    event.getAccountId().toString(),
                    ProductType.ACCOUNT,
                    MovementType.CREDIT,
                    BigDecimal.valueOf(event.getAmount()),
                    BigDecimal.valueOf(0),
                    "",
                    "Initial deposit",
                    event.getSource().toString(),
                    Instant.now()
            );

            createMovementUseCase.execute(movement)
                    .subscribe(
                            saved -> movementEventProducer.publishMovementCreated(movement)
                    );

        }
        catch (Exception e) {
            log.error("Error processing MovementRegisterRequestEvent", e);
        }
    }
}