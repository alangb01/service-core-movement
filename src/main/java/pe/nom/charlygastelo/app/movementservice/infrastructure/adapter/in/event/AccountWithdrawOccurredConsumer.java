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
import pe.nom.charlygastelo.app.shared.avro.dto.AccountWithdrawOccurredEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccountWithdrawOccurredConsumer {

    private final RecordMovementUseCase recordMovementUseCase;

    @KafkaListener(
            topics = "${topic.account-withdraw-occurred}",
            groupId = "movement-service"
    )
    public void consume(AccountWithdrawOccurredEvent event) {

        String txId = event.getTransactionId().toString();
        log.info("[MOVEMENT] ACCOUNT_WITHDRAW_OCCURRED received. txId={}", txId);

        Movement movement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.DEBIT,
                new BigDecimal(event.getAmount()),
                new BigDecimal(event.getBalance()),
                event.getTransactionId().toString(),
                "ACCOUNT_WITHDRAW_OCCURRED",
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
