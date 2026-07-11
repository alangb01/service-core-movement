package pe.nom.charlygastelo.app.movementservice.application.usecase;

import java.math.BigDecimal;
import java.time.Instant;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.exception.InvalidMovementException;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;



@RequiredArgsConstructor
@Slf4j
public class CreateMovementUseCase {

    private final MovementRepositoryPort repository;
    private final MovementEventProducerPort producer;

    public Single<Movement> execute(Movement movement) {
        log.info("Creating movement. product={}, type={}",
                movement.productId(), movement.type());

        if (movement.amount().compareTo(BigDecimal.ZERO) <= 0) {
            return Single.error(new InvalidMovementException("Movement amount must be greater than zero"));
        }

        Movement newMovement = new Movement(
                movement.id(),
                movement.customerId(),
                movement.productId(),
                movement.productType(),
                movement.type(),
                movement.amount(),
                movement.balanceAfter(),
                movement.transactionId(),
                movement.description(),
                movement.sourceService(),
                Instant.now()
        );

        return repository.save(newMovement)
                .flatMap(saved ->
                        producer.publishMovementCreated(saved)
                                .andThen(Single.just(saved))
                );
    }
}