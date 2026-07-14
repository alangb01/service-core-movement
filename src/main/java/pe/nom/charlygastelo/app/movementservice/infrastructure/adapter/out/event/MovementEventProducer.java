package pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event;

import org.apache.avro.specific.SpecificRecordBase;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import io.reactivex.rxjava3.core.Completable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import pe.nom.charlygastelo.app.movementservice.domain.model.Movement;
import pe.nom.charlygastelo.app.movementservice.domain.port.MovementEventProducerPort;
import pe.nom.charlygastelo.app.movementservice.infrastructure.adapter.out.event.mapper.MovementEventMapper;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementCreatedEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementDeletedEvent;
import pe.nom.charlygastelo.app.shared.avro.dto.MovementRecordedEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class MovementEventProducer implements MovementEventProducerPort {

    private final KafkaTemplate<String, SpecificRecordBase> kafkaTemplate;
    private final MovementEventMapper mapper;

    @Value("${topic.movement-created}")
    private String movementCreatedTopic;

    @Value("${topic.movement-deleted}")
    private String movementDeletedTopic;

    @Value("${topic.movement-recorded}")
    private String movementRecordedEvent;

    @Override
    public Completable publishMovementCreated(Movement movement) {
        MovementCreatedEvent event = mapper.toMovementCreatedEvent(movement);
        return publish(movementCreatedTopic, movement.id(), event);
    }

    @Override
    public Completable publishMovementDeleted(Movement movement) {
        MovementDeletedEvent event = mapper.toMovementDeletedEvent(movement);
        return publish(movementDeletedTopic, movement.id(), event);
    }
    @Override
    public Completable publishMovementRecorded(Movement movement) {
        MovementRecordedEvent event = mapper.toMovementRecordedEvent(movement);
        return publish(movementRecordedEvent, movement.id(), event);
    }

    private Completable publish(String topic, String key, SpecificRecordBase event) {
        return Completable.create(emitter -> {
            try {
                kafkaTemplate.send(topic, key, event)
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