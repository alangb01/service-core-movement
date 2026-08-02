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
import pe.nom.charlygastelo.app.shared.avro.dto.AccountYankiPaymentEvent;

@Component
@RequiredArgsConstructor
@Slf4j
public class AccountYankiPaymentEventConsumer {

    private final RecordMovementUseCase recordMovementUseCase;

    @KafkaListener(
            topics = "${topic.account-yanki-payment-occurred}",
            groupId = "movement-service"
    )
    public void consume(AccountYankiPaymentEvent event) {

        String txId = event.getTransactionId().toString();
        log.info("[MOVEMENT] ACCOUNT_YANKI_PAYMENT_OCCURRED received. txId={}", txId);

        Movement movement = new Movement(
                null,
                event.getCustomerId().toString(),
                event.getSourceAccountId().toString(),
                ProductType.ACCOUNT,
                MovementType.DEBIT,
                new BigDecimal(event.getAmount().toString()),
                new BigDecimal(event.getSourceBalance().toString()),
                event.getTransactionId().toString(),
                "ACCOUNT_YANKI_PAYMENT_OCCURRED",
                event.getSource().toString(),
                Instant.now()
        );

        recordMovementUseCase.save(movement)
                .subscribe(
                        () -> log.info("[MOVEMENT] Yanki payment movement recorded. txId={}", txId),
                        error -> log.error("[MOVEMENT] Error recording yanki payment. txId={}, reason={}",
                                txId, error.getMessage(), error)
                );
    }
}
