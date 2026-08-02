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
import pe.nom.charlygastelo.app.shared.avro.dto.AccountTransferOccurredEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccountTransferOccurredConsumer {

    private final RecordMovementUseCase recordMovementUseCase;

    @KafkaListener(
            topics = "${topic.account-transfer-occurred}",
            groupId = "movement-service"
    )
    public void consume(AccountTransferOccurredEvent event) {

        String txId = event.getTransactionId().toString();
        log.info("[MOVEMENT] ACCOUNT_TRANSFER_OCCURRED received. txId={}", txId);

        // DEBIT movement (source)
        Movement debitMovement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getSourceAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.DEBIT,
                new BigDecimal(event.getAmount()),
                new BigDecimal(event.getSourceBalance()),
                event.getTransactionId().toString(),
                "ACCOUNT_TRANSFER_OCCURRED",
                event.getSource().toString(),
                Instant.now()
        );

        // CREDIT movement (target)
        Movement creditMovement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getTargetAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.CREDIT,
                new BigDecimal(event.getAmount()),
                new BigDecimal(event.getTargetBalance()),
                event.getTransactionId().toString(),
                "ACCOUNT_TRANSFER_OCCURRED",
                event.getSource().toString(),
                Instant.now()
        );

        recordMovementUseCase.save(debitMovement)
                .andThen(recordMovementUseCase.save(creditMovement))
                .subscribe(
                        () -> log.info("[MOVEMENT] Transfer movements recorded successfully. txId={}", txId),
                        error -> log.error("[MOVEMENT] Error recording transfer movements. txId={}, reason={}",
                                txId, error.getMessage(), error)
                );
    }
}
