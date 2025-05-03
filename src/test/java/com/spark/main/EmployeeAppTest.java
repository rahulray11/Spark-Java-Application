package com.spark.main;

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase;
import com.spark.pojo.Employee; // Import Employee POJO
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.Test; // Using JUnit 4 as per pom.xml dependencies

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for EmployeeApp, specifically for validateEmployeeNameAndId.
 */
public class EmployeeAppTest extends JavaDatasetSuiteBase {

    @Test
    public void testValidateEmployeeNameAndId_AllValid() {
        // Input data: All employees are valid
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(100, "Charlie", "Sales")
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Same as input since all are valid
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(100, "Charlie", "Sales")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateEmployeeNameAndId_NullName() {
        // Input data: One employee has a null name
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, null, "IT"), // Invalid name
                new Employee(3, "Charlie", "Sales")
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only employees with non-null names
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(3, "Charlie", "Sales")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateEmployeeNameAndId_EmptyName() {
        // Input data: One employee has an empty name
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "", "IT"), // Invalid name
                new Employee(3, "Charlie", "Sales")
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only employees with non-empty names
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(3, "Charlie", "Sales")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateEmployeeNameAndId_InvalidId() {
        // Input data: Employees with ID <= 0
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(0, "Bob", "IT"),   // Invalid ID
                new Employee(-5, "Charlie", "Sales") // Invalid ID
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only employees with ID > 0
        List<Employee> expectedData = Collections.singletonList(
                new Employee(1, "Alice", "HR")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateEmployeeNameAndId_MixedInvalid() {
        // Input data: Mix of invalid names and IDs
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),      // Valid
                new Employee(2, null, "IT"),        // Invalid name
                new Employee(3, "", "Finance"),     // Invalid name
                new Employee(0, "Bob", "IT"),       // Invalid ID
                new Employee(-5, "Charlie", "Sales"), // Invalid ID
                new Employee(6, "David", "Marketing") // Valid
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only valid employees
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(6, "David", "Marketing")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateEmployeeNameAndId_EmptyInput() {
        // Input data: Empty list
        List<Employee> inputData = Collections.emptyList();
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Empty list
        List<Employee> expectedData = Collections.emptyList();
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

     @Test
     public void testValidateEmployeeNameAndId_AllInvalid() {
         // Input data: All employees are invalid
         List<Employee> inputData = Arrays.asList(
                 new Employee(0, "Alice", "HR"),      // Invalid ID
                 new Employee(2, null, "IT"),        // Invalid name
                 new Employee(3, "", "Finance"),     // Invalid name
                 new Employee(-5, "Charlie", "Sales") // Invalid ID
         );
         Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

         // Expected data: Empty list
         List<Employee> expectedData = Collections.emptyList();
         Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

         // Call the method under test
         Dataset<Employee> actualDs = EmployeeApp.validateEmployeeNameAndId(inputDs);

         // Assert dataset equality
         assertDatasetEquals(expectedDs, actualDs);
     }

    // Instance of EmployeeApp for testing the instance method
    private final EmployeeApp employeeAppInstance = new EmployeeApp();

    // --- Tests for validateDepartmentWithPatterns ---

    @Test
    public void testValidateDepartmentWithPatterns_ExactMatches() {
        // Input data: Departments that are exact matches
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),              // Exact match
                new Employee(2, "Bob", "IT"),               // Exact match
                new Employee(3, "Charlie", "Marketing Analytics") // Exact match
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: All should match
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(3, "Charlie", "Marketing Analytics")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateDepartmentWithPatterns(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateDepartmentWithPatterns_RegexMatches() {
        // Input data: Departments matching the "Sales.*" pattern
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "David", "Sales"),           // Regex match
                new Employee(2, "Eve", "Sales Operations"), // Regex match
                new Employee(3, "Frank", "Sales Support")   // Regex match
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: All should match
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "David", "Sales"),
                new Employee(2, "Eve", "Sales Operations"),
                new Employee(3, "Frank", "Sales Support")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateDepartmentWithPatterns(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateDepartmentWithPatterns_NoMatches() {
        // Input data: Departments that do not match any pattern
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Grace", "Finance"),    // No match
                new Employee(2, "Heidi", "Support"),    // No match
                new Employee(3, "Ivan", "Engineering") // No match
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Empty dataset as none should match
        List<Employee> expectedData = Collections.emptyList();
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateDepartmentWithPatterns(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateDepartmentWithPatterns_NullAndEmpty() {
        // Input data: Departments that are null or empty
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Judy", null),       // Null department
                new Employee(2, "Mallory", "")        // Empty department
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Empty dataset as null/empty do not match patterns
        List<Employee> expectedData = Collections.emptyList();
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateDepartmentWithPatterns(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testValidateDepartmentWithPatterns_MixedCases() {
        // Input data: Mix of matching and non-matching departments
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),              // Exact match
                new Employee(2, "Bob", "IT"),               // Exact match
                new Employee(3, "Charlie", "Marketing Analytics"), // Exact match
                new Employee(4, "David", "Sales"),           // Regex match
                new Employee(5, "Eve", "Sales Operations"), // Regex match
                new Employee(6, "Grace", "Finance"),    // No match
                new Employee(7, "Heidi", "Support"),    // No match
                new Employee(8, "Ivan", "Engineering"), // No match
                new Employee(9, "Judy", null),       // Null department
                new Employee(10, "Mallory", "")        // Empty department
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only employees with matching departments
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(3, "Charlie", "Marketing Analytics"),
                new Employee(4, "David", "Sales"),
                new Employee(5, "Eve", "Sales Operations")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the method under test
        Dataset<Employee> actualDs = EmployeeApp.validateDepartmentWithPatterns(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    // --- Tests for processEmployeeDataset (Instance Method) ---

    @Test
    public void testProcessEmployeeDataset_AllValid() {
        // Input data: All employees fully valid
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(3, "Charlie", "Sales Operations"),
                new Employee(4, "Diana", "Marketing Analytics")
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Same as input
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(2, "Bob", "IT"),
                new Employee(3, "Charlie", "Sales Operations"),
                new Employee(4, "Diana", "Marketing Analytics")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the instance method under test
        Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testProcessEmployeeDataset_InvalidNameOrId() {
        // Input data: Some employees have invalid names or IDs, but valid departments
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),          // Valid
                new Employee(0, "Bob", "IT"),           // Invalid ID
                new Employee(3, null, "Sales"),         // Invalid Name
                new Employee(4, "", "Marketing Analytics") // Invalid Name
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only the employee with valid name and ID
        List<Employee> expectedData = Collections.singletonList(
                new Employee(1, "Alice", "HR")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the instance method under test
        Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }


    @Test
    public void testProcessEmployeeDataset_InvalidDepartment() {
        // Input data: Employees have valid names/IDs but invalid departments (null, empty, or non-matching pattern)
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),          // Valid
                new Employee(2, "Bob", null),           // Invalid Dept (null)
                new Employee(3, "Charlie", ""),         // Invalid Dept (empty)
                new Employee(4, "Diana", "Engineering") // Invalid Dept (pattern)
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only the employee with a valid department
        List<Employee> expectedData = Collections.singletonList(
                new Employee(1, "Alice", "HR")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the instance method under test
        Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testProcessEmployeeDataset_MixedInvalidCases() {
        // Input data: Mix of various invalid cases
        List<Employee> inputData = Arrays.asList(
                new Employee(1, "Alice", "HR"),              // Valid
                new Employee(0, "Bob", "IT"),               // Invalid ID, Valid Dept
                new Employee(3, null, "Sales Operations"), // Invalid Name, Valid Dept
                new Employee(4, "Diana", "Engineering"),     // Valid Name/ID, Invalid Dept Pattern
                new Employee(5, "Eve", null),           // Valid Name/ID, Invalid Dept (null)
                new Employee(-1, "", ""),               // Invalid ID, Invalid Name, Invalid Dept (empty)
                new Employee(7, "Frank", "Sales Support")    // Valid
        );
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Only employees passing all validations
        List<Employee> expectedData = Arrays.asList(
                new Employee(1, "Alice", "HR"),
                new Employee(7, "Frank", "Sales Support")
        );
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the instance method under test
        Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

    @Test
    public void testProcessEmployeeDataset_EmptyInput() {
        // Input data: Empty list
        List<Employee> inputData = Collections.emptyList();
        Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Empty list
        List<Employee> expectedData = Collections.emptyList();
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

        // Call the instance method under test
        Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

        // Assert dataset equality
        assertDatasetEquals(expectedDs, actualDs);
    }

     @Test
     public void testProcessEmployeeDataset_AllInvalid() {
         // Input data: All employees fail at least one validation step
         List<Employee> inputData = Arrays.asList(
                 new Employee(0, "Bob", "IT"),               // Invalid ID
                 new Employee(3, null, "Sales Operations"), // Invalid Name
                 new Employee(4, "Diana", "Engineering"),     // Invalid Dept Pattern
                 new Employee(5, "Eve", null),           // Invalid Dept (null)
                 new Employee(-1, "", "")               // Invalid ID, Name, Dept
         );
         Dataset<Employee> inputDs = spark().createDataset(inputData, Encoders.bean(Employee.class));

        // Expected data: Empty list
        List<Employee> expectedData = Collections.emptyList();
        Dataset<Employee> expectedDs = spark().createDataset(expectedData, Encoders.bean(Employee.class));

         // Call the instance method under test
         Dataset<Employee> actualDs = employeeAppInstance.processEmployeeDataset(inputDs);

         // Assert dataset equality
         assertDatasetEquals(expectedDs, actualDs);
     }
}
