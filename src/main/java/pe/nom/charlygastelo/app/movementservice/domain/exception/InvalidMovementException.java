package pe.nom.charlygastelo.app.movementservice.domain.exception;

public class InvalidMovementException extends RuntimeException {
    public InvalidMovementException(String message) {
        super(message);
    }
}