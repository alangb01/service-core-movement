package pe.nom.charlygastelo.app.movementservice.infrastructure.events;


import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.reactivex.rxjava3.core.Completable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.infrastructure.events.mapper.MovementEventMapper;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementEventProducer implements MovementEventProducerPort {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AvroJsonSerializer avroJsonSerializer;
    private final MovementEventMapper mapper;

    @Value("${topic.movement-created}")
    private String movementCreatedTopic;

    @Value("${topic.movement-deleted}")
    private String movementDeletedTopic;

    @Override
    public Completable publishMovementCreated(Movement movement) {
        return publish(movementCreatedTopic, movement.id(), mapper.toMovementCreatedEvent(movement));
    }

    @Override
    public Completable publishMovementDeleted(Movement movement) {
        return publish(movementDeletedTopic, movement.id(), mapper.toMovementDeletedEvent(movement));
    }

    private Completable publish(String topic, String key, SpecificRecordBase event) {
        return Completable.create(emitter -> {
            try {
                String payload = avroJsonSerializer.serialize(event);

                kafkaTemplate.send(topic, key, payload)
                        .whenComplete((result, error) -> {
                            if (error != null) {
                                emitter.onError(error);
                            }
                            else {
                                log.info("Movement event published. topic={}, key={}", topic, key);
                                emitter.onComplete();
                            }
                        });

            }
            catch (Exception e) {
                emitter.onError(e);
            }
        });
    }
}