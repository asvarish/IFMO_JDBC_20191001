package com.efimchick.ifmo.web.jdbc;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.LinkedHashSet;
import java.util.Set;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return resultSet -> {
            Set<Employee> employees;
            employees = new LinkedHashSet<>();

            try {
                while (resultSet.next()){
                    employees.add(getEmployee(resultSet));
                }
            } catch (SQLException e) {
                return null;
            }
            return employees;
        };
    }

    private Employee getEmployee(ResultSet resultSet) throws SQLException {
        try {
            Employee employee = new Employee(
                    new BigInteger(
                            resultSet.getString("ID")),
                    new FullName(
                            resultSet.getString("FIRSTNAME"),
                            resultSet.getString("LASTNAME"),
                            resultSet.getString("MIDDLENAME")),
                    Position.valueOf(resultSet.getString("POSITION")),
                    LocalDate.parse(resultSet.getString("HIREDATE")),
                    resultSet.getBigDecimal("SALARY"),
                    getManager(resultSet, resultSet.getInt("MANAGER")));

            return employee;
        } catch (SQLException e) {
            return null;
        }
    }

    public Employee getManager(ResultSet resultSet, int managerId) throws SQLException {
        Employee manager = null;
        int rowNumber = resultSet.getRow();

        try {
            resultSet.beforeFirst();

            while (resultSet.next()) {
                int id = resultSet.getInt("ID");
                if (id == managerId) {
                    manager = getEmployee(resultSet);
                    break;
                }
            }

            resultSet.absolute(rowNumber);
        } catch (SQLException e) {
            return null;
        }

        return manager;
    }
}
