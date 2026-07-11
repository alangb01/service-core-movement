package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.persistence;

import java.time.LocalDateTime;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import reactor.core.publisher.Flux;

public interface MovementReactiveRepository
        extends ReactiveMongoRepository<MovementDocument, String> {

    Flux<MovementDocument> findByProductIdAndProductType(String productId, ProductType productType);

    Flux<MovementDocument> findByProductIdAndCreatedAtBetween(
            String productId,
            LocalDateTime start,
            LocalDateTime end
    );

    Flux<MovementDocument> findTop10ByProductIdOrderByCreatedAtDesc(String productId);
}