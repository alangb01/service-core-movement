package pe.nom.charlygastelo.app.movementservice.application.usecase;

import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@RequiredArgsConstructor
@Slf4j
public class ListLastMovementsUseCase {

    private final MovementRepositoryPort repository;

    public Flowable<Movement> last10ByProduct(String productId) {
        log.info("Listing last 10 movements for product {}", productId);
        return repository.findTop10ByProductIdOrderByCreatedAtDesc(productId);
    }
}