package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class LibraryEvent {
    @NotNull
    private Integer libraryEventId;
    @NotNull
    private LibraryEventType libraryEventType;
    @NotNull
    @Valid
    private Book book;
}
