package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest;


import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.application.usecase.*;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.mapper.MovementRestMapper;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.request.CreateMovementRequest;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.response.MovementResponse;

@RestController
@RequestMapping("/movements")
@SecurityRequirement(name = "bearerAuth")
@RequiredArgsConstructor
public class MovementController {

    private final CreateMovementUseCase createUseCase;
    private final GetMovementUseCase getUseCase;
    private final ListMovementsUseCase listUseCase;
    private final DeleteMovementUseCase deleteUseCase;
    private final MovementRestMapper mapper;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Single<MovementResponse> create(@RequestBody CreateMovementRequest request) {
        return createUseCase.execute(mapper.toDomain(request))
                .map(mapper::toResponse);
    }

    @GetMapping("/{id}")
    public Single<MovementResponse> findById(@PathVariable String id) {
        return getUseCase.byId(id)
                .map(mapper::toResponse)
                .toSingle();
    }

    @GetMapping
    public Flowable<MovementResponse> findAll() {
        return listUseCase.all()
                .map(mapper::toResponse);
    }

    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Completable delete(@PathVariable String id) {
        return deleteUseCase.execute(id);
    }
}