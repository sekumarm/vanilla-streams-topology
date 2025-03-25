package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MergedRecord {
    private int studentId;
    private List<Topic1Record> topic1Records;
    private List<Topic2Record> topic2Records;
}
