package pe.nom.charlygastelo.app.movementservice.domain.port;

import java.time.LocalDateTime;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;



public interface MovementRepositoryPort {

    Single<Movement> save(Movement movement);

    Maybe<Movement> findById(String id);

    Flowable<Movement> findAll();

    Flowable<Movement> findByCustomerId(String customerId);

    Flowable<Movement> findByProductId(String productId);

    Flowable<Movement> findByProductIdAndCreatedAtBetween(
            String productId,
            LocalDateTime start,
            LocalDateTime end
    );

    Flowable<Movement> findTop10ByProductIdOrderByCreatedAtDesc(String productId);

    Completable deleteById(String id);
}