package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.persistence;

import java.time.LocalDateTime;

import org.springframework.stereotype.Repository;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;
import reactor.adapter.rxjava.RxJava3Adapter;

@Repository
@RequiredArgsConstructor
public class MovementRepositoryAdapter implements MovementRepositoryPort {

    private final MovementReactiveRepository repository;
    private final MovementPersistenceMapper mapper;

    @Override
    public Single<Movement> save(Movement movement) {
        return RxJava3Adapter.monoToSingle(
                repository.save(mapper.toDocument(movement))
        ).map(mapper::toDomain);
    }

    @Override
    public Maybe<Movement> findById(String id) {
        return RxJava3Adapter.monoToMaybe(repository.findById(id))
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findAll() {
        return RxJava3Adapter.fluxToFlowable(repository.findAll())
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findByCustomerId(String customerId) {
        return RxJava3Adapter.fluxToFlowable(repository.findByCustomerId(customerId))
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findByProductId(String productId) {
        return RxJava3Adapter.fluxToFlowable(repository.findByProductId(productId))
                .map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findByProductIdAndCreatedAtBetween(
            String productId,
            LocalDateTime start,
            LocalDateTime end) {

        return RxJava3Adapter.fluxToFlowable(
                repository.findByProductIdAndCreatedAtBetween(productId, start, end)
        ).map(mapper::toDomain);
    }

    @Override
    public Flowable<Movement> findTop10ByProductIdOrderByCreatedAtDesc(String productId) {
        return RxJava3Adapter.fluxToFlowable(
                repository.findTop10ByProductIdOrderByCreatedAtDesc(productId)
        ).map(mapper::toDomain);
    }

    @Override
    public Completable deleteById(String id) {
        return RxJava3Adapter.monoToCompletable(repository.deleteById(id));
    }
}