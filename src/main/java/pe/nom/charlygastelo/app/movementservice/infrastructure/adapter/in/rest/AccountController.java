package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest;

import org.springframework.web.bind.annotation.*;
import io.reactivex.rxjava3.core.Flowable;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import lombok.RequiredArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.application.usecase.*;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.mapper.MovementRestMapper;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.in.rest.response.MovementResponse;

@RestController
@RequestMapping("/api/accounts/{accountId}")
@SecurityRequirement(name = "bearerAuth")
@RequiredArgsConstructor
public class AccountController {
    private final ListMovementsUseCase listUseCase;
    private final MovementRestMapper mapper;

    @GetMapping("/movements")
    public Flowable<MovementResponse> findByAccount(@PathVariable String accountId) {
        return listUseCase.byProduct(accountId, ProductType.ACCOUNT)
                .map(mapper::toResponse);
    }
}