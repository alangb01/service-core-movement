package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.persistence;

import java.math.BigDecimal;
import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import pe.nom.charlygastelo.app.movementservice.domain.model.MovementType;
import pe.nom.charlygastelo.app.movementservice.domain.model.ProductType;

@Document(collection = "movements")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MovementDocument {

    @Id
    private String id;

    private String customerId;
    private String productId;
    private ProductType productType;
    private MovementType type;

    private BigDecimal amount;
    private BigDecimal balanceAfter;

    private String transactionId;
    private String description;
    private String sourceService;

    private Instant createdAt;
}