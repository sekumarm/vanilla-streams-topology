package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Topic2Record {
    private String degree;
    private boolean authorized;
    private int studentId;
    private String collegeName;
}
