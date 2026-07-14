package pe.nom.charlygastelo.app.movementservice.domain.port;

import io.reactivex.rxjava3.core.Completable;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;

public interface MovementEventProducerPort {

    Completable publishMovementCreated(Movement movement);

    Completable publishMovementDeleted(Movement movement);

    Completable publishMovementRecorded(Movement movement);
}