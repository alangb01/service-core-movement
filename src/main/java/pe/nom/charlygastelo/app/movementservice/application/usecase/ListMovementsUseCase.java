package pe.nom.charlygastelo.app.movementservice.application.usecase;

import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@RequiredArgsConstructor
@Slf4j
public class ListMovementsUseCase {

    private final MovementRepositoryPort repository;

    public Flowable<Movement> all() {
        log.info("Listing all movements");
        return repository.findAll();
    }

    public Flowable<Movement> byProduct(String productId, ProductType productType) {
        log.info("Listing movements for product {}", productId);
        return repository.findByProductIdAndProductType(productId, productType);
    }
}