package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MergedRecord {
    // Fields from Topic1Record
    private String name;
    private int age;
    private String nationality;
    private int studentId;
    
    // Fields from Topic2Record
    private String degree;
    private boolean authorized;
    private String collegeName;
}
