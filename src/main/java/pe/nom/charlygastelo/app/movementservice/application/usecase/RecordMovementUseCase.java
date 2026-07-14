package pe.nom.charlygastelo.app.movementservice.application.usecase;

import org.springframework.stereotype.Component;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@Component
@RequiredArgsConstructor
@Slf4j
public class RecordMovementUseCase {

    private final MovementRepositoryPort movementRepository;
    private final MovementEventProducerPort producer;

    public Completable save(Movement movement) {

        return movementRepository.save(movement)
                .flatMap( m ->
                    producer.publishMovementRecorded(m).andThen(Single.just(m))
                )
                .doOnSuccess(savedMovement ->
                        log.info("[MOVEMENT] Movement saved. txId={}, type={}",
                                savedMovement.transactionId(),
                                savedMovement.type())
                ).doOnError(throwable ->
                        log.error("[MOVEMENT] Error saving movement. txId={}, type={}",
                                throwable.getMessage(),
                                movement.transactionId(),
                                movement.type())
                ).ignoreElement();
    }
}
