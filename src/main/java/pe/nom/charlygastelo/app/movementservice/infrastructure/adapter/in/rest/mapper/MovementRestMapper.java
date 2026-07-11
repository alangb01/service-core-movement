package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.mapper;

import java.time.Instant;
import org.springframework.stereotype.Component;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.model.MovementType;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.request.CreateMovementRequest;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.response.MovementResponse;

@Component
public class MovementRestMapper {

    public Movement toDomain(CreateMovementRequest request) {
        return new Movement(
                null,
                request.customerId(),
                request.productId(),
                ProductType.valueOf(request.productType()),
                MovementType.valueOf(request.type()),
                request.amount(),
                request.balanceAfter(),
                request.transactionId(),
                request.description(),
                request.sourceService(),
                Instant.now()
        );
    }

    public MovementResponse toResponse(Movement movement) {
        return new MovementResponse(
                movement.id(),
                movement.customerId(),
                movement.productId(),
                movement.productType().name(),
                movement.type().name(),
                movement.amount(),
                movement.balanceAfter(),
                movement.transactionId(),
                movement.description(),
                movement.sourceService(),
                movement.createdAt()
        );
    }
}