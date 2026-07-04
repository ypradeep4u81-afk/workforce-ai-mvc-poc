package workforce_ai_mvc_poc.domain;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record ShiftAssignment(
    Long id, 
    String employeeId, 
    LocalDate shiftDate, 
    String role, 
    LocalDateTime createdAt
) {}
