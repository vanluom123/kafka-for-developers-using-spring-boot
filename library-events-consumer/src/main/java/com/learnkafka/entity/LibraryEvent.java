package com.learnkafka.entity;

import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Column;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
@Entity
@Table(name = "library_events")
public class LibraryEvent {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "library_event_id")
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    @Column(name = "library_event_type")
    private LibraryEventType libraryEventType;
    
    @OneToOne(mappedBy = "libraryEvent", cascade = { CascadeType.ALL })
    @ToString.Exclude
    private Book book;
}
