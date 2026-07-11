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
@RequestMapping("/credit-cards/{creditCardId}")
@SecurityRequirement(name = "bearerAuth")
@RequiredArgsConstructor
public class CreditCardController {
    private final ListMovementsUseCase listUseCase;
    private final MovementRestMapper mapper;

    @GetMapping("/movements")
    public Flowable<MovementResponse> findByCard(@PathVariable String cardId) {
        return listUseCase.byProduct(cardId, ProductType.CREDIT_CARD)
                .map(mapper::toResponse);
    }
}