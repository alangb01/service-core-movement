package pe.nom.charlygastelo.app.movementservice.domain.exception;

public class MovementNotFoundException extends RuntimeException {
    public MovementNotFoundException(String message) {
        super(message);
    }
}