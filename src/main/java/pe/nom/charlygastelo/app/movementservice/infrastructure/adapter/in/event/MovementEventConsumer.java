package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.event;

import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.application.usecase.CreateMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event.MovementEventProducer;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementEventConsumer {

    private final CreateMovementUseCase createMovementUseCase;
    private final MovementEventProducer movementEventProducer;
//
//    @KafkaListener(topics = "${topic.movement-register-request}", groupId = "movement-service")
//    public void consume(MovementCreateRequestEvent event) {
//        try {
//            Movement movement = new Movement(
//                    null,
//                    event.getCustomerId().toString(),
//                    event.getProductId().toString(),
//                    ProductType.valueOf(event.getProductType().toString()),
//                    MovementType.valueOf(event.getMovementType().toString()),
//                    BigDecimal.valueOf(event.getAmount()),
//                    BigDecimal.valueOf(event.getBalanceAfter()),
//                    event.getTransactionId().toString(),
//                    event.getDescription().toString(),
//                    event.getSource().toString(),
//                    Instant.now()
//            );
//
//            createMovementUseCase.execute(movement)
//                .subscribe(
//                    saved -> movementEventProducer.publishMovementCreated(movement)
//                );
//
//        }
//        catch (Exception e) {
//            log.error("Error processing MovementRegisterRequestEvent", e);
//        }
//    }
//
//    @KafkaListener(topics = "${topic.movement-initial-deposit-request}", groupId = "movement-service")
//    public void consume(AccountInitialDepositEvent event) {
//        try {
//            Movement movement = new Movement(
//                    null,
//                    event.getCustomerId().toString(),
//                    event.getAccountId().toString(),
//                    ProductType.ACCOUNT,
//                    MovementType.CREDIT,
//                    BigDecimal.valueOf(event.getAmount()),
//                    BigDecimal.valueOf(0),
//                    "",
//                    "Initial deposit",
//                    event.getSource().toString(),
//                    Instant.now()
//            );
//
//            createMovementUseCase.execute(movement)
//                    .subscribe(
//                            saved -> movementEventProducer.publishMovementCreated(movement)
//                    );
//
//        }
//        catch (Exception e) {
//            log.error("Error processing MovementRegisterRequestEvent", e);
//        }
//    }
}