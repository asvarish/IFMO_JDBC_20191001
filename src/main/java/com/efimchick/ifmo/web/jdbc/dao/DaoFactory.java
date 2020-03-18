package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

public class DaoFactory {

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                List<Employee> list = new LinkedList<>();
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId());
                assert rs != null;
                list = employeelist(rs);
                return list;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> list = new LinkedList<>();
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + employee.getId());
                assert rs != null;
                list = employeelist(rs);
                return list;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                try {
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE ID=" + Id.toString());
                    assert rs != null;
                    if (rs.next())
                        return Optional.of(employee(rs));
                    else
                        return Optional.empty();
                } catch (SQLException ex) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Employee> getAll() {
                List<Employee> list = new LinkedList<>();
                ResultSet rs = getRs("SELECT * FROM EMPLOYEE");
                assert rs != null;
                list = employeelist(rs);
                return list;
            }

            @Override
            public Employee save(Employee employee) {
                try {
                    ConnectionSource connectionSource = ConnectionSource.instance();
                    Connection connection = connectionSource.createConnection();
                    Statement stmnt = connection.createStatement();
                    String query = "INSERT INTO EMPLOYEE (ID, FIRSTNAME, LASTNAME, MIDDLENAME, POSITION, MANAGER, HIREDATE, SALARY, DEPARTMENT) VALUES (" + employee.getId().toString() + ",'" + employee.getFullName().getFirstName() + "','"
                            + employee.getFullName().getLastName() + "','" + employee.getFullName().getMiddleName() + "','" + employee.getPosition().toString() +
                            "'," + employee.getManagerId().toString() + ",'" + Date.valueOf(employee.getHired()).toString() + "'," + employee.getSalary().toString()
                            + "," + employee.getDepartmentId().toString() + ")";
                    stmnt.executeUpdate(query);
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                try {
                    ConnectionSource connectionSource = ConnectionSource.instance();
                    Connection connection = connectionSource.createConnection();
                    Statement stmnt = connection.createStatement();
                    stmnt.execute("DELETE FROM EMPLOYEE WHERE ID=" + employee.getId().toString());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private ResultSet getRs(String query) {
        try {
            ConnectionSource connectionSource = ConnectionSource.instance();
            Connection connection = connectionSource.createConnection();
            Statement stmnt = connection.createStatement();
            return stmnt.executeQuery(query);
        } catch (SQLException e) {
            return null;
        }
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                try {
                    ResultSet rs = getRs("SELECT * FROM DEPARTMENT WHERE ID=" + Id.toString());
                    if (rs.next() == true)
                        return Optional.of(department(rs));
                    else
                        return Optional.empty();
                } catch (SQLException e) {
                    return Optional.empty();
                }
            }

            @Override
            public List<Department> getAll() {
                List<Department> departments_list = new LinkedList<>();
                try {
                    ResultSet rs = getRs("SELECT * FROM DEPARTMENT");
                    while (rs.next()) {
                        departments_list.add(department(rs));
                    }
                    return departments_list;
                } catch (SQLException e) {
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                try {
                    PreparedStatement stmnt;
                    String query;
                    if (getById(department.getId()).equals(Optional.empty())) {
                        query = "INSERT INTO DEPARTMENT VALUES (?,?,?)";
                        stmnt = prepStatement(query);
                        stmnt.setString(2, department.getName());
                        stmnt.setInt(1, department.getId().intValue());
                        stmnt.setString(3, department.getLocation());
                    } else {
                        query = "UPDATE DEPARTMENT SET NAME=?, LOCATION=? where ID=?";
                        stmnt = prepStatement(query);
                        stmnt.setString(2, department.getLocation());
                        stmnt.setString(1, department.getName());
                        stmnt.setInt(3, department.getId().intValue());
                    }
                    int rows = stmnt.executeUpdate();
                    return department;
                } catch (SQLException eX) {
                    return null;
                }
            }

            PreparedStatement prepStatement(String query) {
                try {
                    ConnectionSource consrc = ConnectionSource.instance();
                    Connection con = consrc.createConnection();
                    PreparedStatement stmnt = con.prepareStatement(query);
                    return stmnt;
                } catch (SQLException ex) {
                    return null;
                }
            }

            @Override
            public void delete(Department dep) {
                try {
                    ConnectionSource consrc = ConnectionSource.instance();
                    Connection con = consrc.createConnection();
                    Statement stmnt = con.createStatement();
                    stmnt.executeUpdate("delete from department where id=" + dep.getId().toString());
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        };
    }

    private List<Employee> employeelist(ResultSet rs) {
        List<Employee> employees = new LinkedList<>();
        try {
            while (rs.next()) {
                employees.add(employee(rs));
            }
        } catch (SQLException ex) {
            return new ArrayList<>();
        }
        return employees;
    }

    private Employee employee(ResultSet rs) {
        try {
            String managerStr = rs.getString("MANAGER");
            BigInteger managerID;
            if (managerStr == null) {
                managerID = BigInteger.ZERO;
            } else {
                managerID = new BigInteger(rs.getString("MANAGER"));
            }

            String depStr = rs.getString("DEPARTMENT");
            BigInteger depID;
            if (depStr == null) {
                depID = BigInteger.ZERO;
            } else {
                depID = new BigInteger(rs.getString("DEPARTMENT"));
            }

            return new Employee(
                    new BigInteger(rs.getString("ID")),
                    new FullName(
                            rs.getString("FIRSTNAME"),
                            rs.getString("LASTNAME"),
                            rs.getString("MIDDLENAME")
                    ),
                    Position.valueOf(rs.getString("POSITION")),
                    LocalDate.parse(rs.getString("HIREDATE")),
                    new BigDecimal(rs.getString("SALARY")),
                    managerID,
                    depID
            );
        } catch (SQLException ex) {
            return null;
        }
    }

    private Department department(ResultSet rs) {
        try {
            BigInteger depId = new BigInteger(rs.getString("ID"));
            String name = rs.getString("NAME");
            String loc = rs.getString("LOCATION");
            return new Department(depId, name, loc);
        } catch (SQLException ex) {
            return new Department(BigInteger.ZERO, "", "");
        }
    }
}
