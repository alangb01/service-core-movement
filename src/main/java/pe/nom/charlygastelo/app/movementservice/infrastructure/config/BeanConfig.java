package pe.nom.charlygastelo.app.movementservice.infrastructure.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.application.usecase.CreateMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.application.usecase.DeleteMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.application.usecase.GetMovementUseCase;
import pe.nom.charlygastelo.app.movementservice.application.usecase.ListMovementsUseCase;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementRepositoryPort;

@Configuration
@RequiredArgsConstructor
public class BeanConfig {

    @Bean
    public CreateMovementUseCase createMovementUseCase(
            MovementRepositoryPort repository,
            MovementEventProducerPort producer) {

        return new CreateMovementUseCase(
                repository,
                producer
        );
    }

    @Bean
    public GetMovementUseCase getMovementUseCase(
            MovementRepositoryPort repository) {

        return new GetMovementUseCase(repository);
    }

    @Bean
    public ListMovementsUseCase listMovementsUseCase(
            MovementRepositoryPort repository) {

        return new ListMovementsUseCase(repository);
    }


    @Bean
    public DeleteMovementUseCase deleteMovementUseCase(
            MovementRepositoryPort repository,
            MovementEventProducerPort producer) {

        return new DeleteMovementUseCase(
                repository,
                producer
        );
    }

}