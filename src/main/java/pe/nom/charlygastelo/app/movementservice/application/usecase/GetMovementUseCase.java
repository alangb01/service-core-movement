package pe.nom.charlygastelo.app.movementservice.application.usecase;

import io.reactivex.rxjava3.core.Maybe;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.domain.exception.MovementNotFoundException;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@RequiredArgsConstructor
public class GetMovementUseCase {

    private final MovementRepositoryPort repository;

    public Maybe<Movement> byId(String id) {
        return repository.findById(id)
                .switchIfEmpty(Maybe.error(
                        new MovementNotFoundException("Movement not found: " + id)
                ));
    }
}