package pe.nom.charlygastelo.app.movementservice.application.usecase;

import java.time.LocalDateTime;

import io.reactivex.rxjava3.core.Flowable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@RequiredArgsConstructor
@Slf4j
public class ListMovementsUseCase {

    private final MovementRepositoryPort repository;

    public Flowable<Movement> all() {
        log.info("Listing all movements");
        return repository.findAll();
    }

    public Flowable<Movement> byCustomer(String customerId) {
        log.info("Listing movements for customer {}", customerId);
        return repository.findByCustomerId(customerId);
    }

    public Flowable<Movement> byProduct(String productId) {
        log.info("Listing movements for product {}", productId);
        return repository.findByProductId(productId);
    }

    public Flowable<Movement> byProductBetween(
            String productId,
            LocalDateTime start,
            LocalDateTime end) {

        log.info("Listing movements for product {} between {} and {}",
                productId, start, end);

        return repository.findByProductIdAndCreatedAtBetween(productId, start, end);
    }
}