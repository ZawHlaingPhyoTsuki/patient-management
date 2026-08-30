package com.pm.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "patient", groupId = "analytics-service")
//    public void consumeEvent(byte[] event) {
    public void consumeEvent(ConsumerRecord<String, byte[]> record) {
        try {
            // Get raw bytes directly from the record
            byte[] eventBytes = record.value();

            if (eventBytes == null) {
                return;
            }

            PatientEvent patientEvent = PatientEvent.parseFrom(eventBytes);
            // ----- perform any business related to analytics here

            log.info("Received Patient Event: [PatientId={}, PatientName={}, " + "PatientEmail={}]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail());
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserializing event {}", e.getMessage());
        }
    }
}
