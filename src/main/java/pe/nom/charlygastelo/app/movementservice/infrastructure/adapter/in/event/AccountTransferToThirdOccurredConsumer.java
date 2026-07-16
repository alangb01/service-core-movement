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
import pe.nom.charlygastelo.app.shared.avro.dto.AccountTransferToThirdOccurredEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccountTransferToThirdOccurredConsumer {

    private final RecordMovementUseCase recordMovementUseCase;

    @KafkaListener(
            topics = "${topic.account-transfer-to-third-occurred}",
            groupId = "movement-service"
    )
    public void consume(AccountTransferToThirdOccurredEvent event) {

        String txId = event.getTransactionId().toString();
        log.info("[MOVEMENT] ACCOUNT_TRANSFER_TO_THIRD_OCCURRED received. txId={}", txId);

        Movement debitMovement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getSourceAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.DEBIT,
                new BigDecimal(event.getAmount().toString()),
                new BigDecimal(event.getSourceBalance().toString()),
                event.getTransactionId().toString(),
                "ACCOUNT_TRANSFER_TO_THIRD_OCCURRED",
                event.getSource().toString(),
                Instant.now()
        );

        Movement creditMovement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getThirdPartyAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.CREDIT,
                new BigDecimal(event.getAmount().toString()),
                new BigDecimal(event.getThirdPartyBalance().toString()),
                event.getTransactionId().toString(),
                "ACCOUNT_TRANSFER_TO_THIRD_OCCURRED",
                event.getSource().toString(),
                Instant.now()
        );

        recordMovementUseCase.save(debitMovement)
                .andThen(recordMovementUseCase.save(creditMovement))
                .subscribe(
                        () -> log.info("[MOVEMENT] Third-party transfer movements recorded. txId={}", txId),
                        error -> log.error("[MOVEMENT] Error recording third-party transfer. txId={}, reason={}",
                                txId, error.getMessage(), error)
                );
    }
}
