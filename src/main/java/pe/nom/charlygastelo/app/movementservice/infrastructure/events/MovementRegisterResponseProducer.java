package pe.nom.charlygastelo.app.movementservice.infrastructure.events;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementRegisterResponseEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementRegisterResponseProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AvroJsonSerializer avroJsonSerializer;

    @Value("${topic.movement-register-response}")
    private String movementRegisterResponseTopic;

    public void publish(String correlationId, MovementRegisterResponseEvent event) {
        try {
            String payload = avroJsonSerializer.serialize(event);

            kafkaTemplate.send(movementRegisterResponseTopic, correlationId, payload)
                    .whenComplete((result, error) -> {
                        if (error != null) {
                            log.error("Error publishing MovementRegisterResponseEvent", error);
                        }
                        else {
                            log.info("MovementRegisterResponseEvent published. correlationId={}", correlationId);
                        }
                    });

        }
        catch (Exception e) {
            log.error("Error serializing MovementRegisterResponseEvent", e);
        }
    }
}