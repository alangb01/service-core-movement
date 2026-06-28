package pe.nom.charlygastelo.app.movementservice.application.usecase;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.domain.exception.MovementNotFoundException;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@RequiredArgsConstructor
public class DeleteMovementUseCase {

    private final MovementRepositoryPort repository;
    private final MovementEventProducerPort producer;

    public Completable execute(String id) {
        return repository.findById(id)
                .switchIfEmpty(Single.error(
                        new MovementNotFoundException("Movement not found: " + id)
                ))
                .flatMapCompletable(movement ->
                        repository.deleteById(id)
                                .andThen(producer.publishMovementDeleted(movement))
                );
    }
}