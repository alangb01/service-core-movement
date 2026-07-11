package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.persistence;

import java.time.LocalDateTime;
import org.springframework.stereotype.Repository;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@Repository
@RequiredArgsConstructor
@Slf4j
public class MovementRepositoryAdapter implements MovementRepositoryPort {

    private final MovementReactiveRepository repository;
    private final MovementPersistenceMapper mapper;

    @Override
    public Single<Movement> save(Movement movement) {
        return Single.fromPublisher(
                repository.save(mapper.toDocument(movement))
        ).map(mapper::toDomain);
    }

    @Override
    public Maybe<Movement> findById(String id) {
        return Maybe.fromPublisher(repository.findById(id))
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findAll() {
        return Flowable.fromPublisher(repository.findAll())
                .map(mapper::toDomain);
    }


    @Override
    public Flowable<Movement> findByProductIdAndProductType(String productId, ProductType productType) {
        return Flowable.fromPublisher(repository.findByProductIdAndProductType(productId, productType))
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findByProductIdAndCreatedAtBetween(
            String productId,
            LocalDateTime start,
            LocalDateTime end) {

        return Flowable.fromPublisher(
                repository.findByProductIdAndCreatedAtBetween(productId, start, end)
        ).map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findTop10ByProductIdOrderByCreatedAtDesc(String productId) {
        return Flowable.fromPublisher(
                repository.findTop10ByProductIdOrderByCreatedAtDesc(productId)
        ).map(mapper::toDomain);
    }

    @Override
    public Completable deleteById(String id) {
        return Completable.fromPublisher(repository.deleteById(id));
    }
}