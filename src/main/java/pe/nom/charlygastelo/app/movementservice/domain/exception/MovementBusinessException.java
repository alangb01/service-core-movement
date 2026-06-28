package pe.nom.charlygastelo.app.movementservice.domain.exception;

public class MovementBusinessException extends RuntimeException {
    public MovementBusinessException(String message) {
        super(message);
    }
}