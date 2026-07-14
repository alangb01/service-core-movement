package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.event;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.application.usecase.RecordMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.MovementType;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event.mapper.MovementEventMapper;
import pe.nom.charlygastelo.app.shared.avro.dto.AccountDepositOccurredEvent;


@Component
@RequiredArgsConstructor
@Slf4j
public class AccountDepositOccurredConsumer {

    private final RecordMovementUseCase recordMovementUseCase;
    private final MovementEventMapper mapper;

    @KafkaListener(
            topics = "${topic.account-deposit-occurred}",
            groupId = "movement-service"
    )
    public void consume(AccountDepositOccurredEvent event) {

        String txId = event.getTransactionId().toString();
        log.info("[MOVEMENT] ACCOUNT_DEPOSIT_OCCURRED received. txId={}", txId);

        Movement movement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.CREDIT,
                new BigDecimal(event.getAmount().toString()),
                new BigDecimal(event.getBalance().toString()),
                event.getTransactionId().toString(),
                "ACCOUNT_DEPOSIT_OCCURRED",
                event.getSource().toString(),
                Instant.now()

        );

        recordMovementUseCase.save(movement)
                .subscribe(
                        () -> log.info("[MOVEMENT] Movement recorded successfully. txId={}", txId),
                        error -> log.error("[MOVEMENT] Error recording movement. txId={}, reason={}",
                                txId, error.getMessage(), error)
                );

    }
}
