package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.*;

public class ServiceFactory {

    private ResultSet getRs(String query) {
        try {
            ConnectionSource connectionSource = ConnectionSource.instance();
            Connection connection = connectionSource.createConnection();
            Statement stmnt = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            return stmnt.executeQuery(query);
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> employee() {
        List<Employee> employees = new LinkedList<>();
        try {
            ResultSet rs = getRs("SELECT * FROM EMPLOYEE");
            while (rs.next()) {
                employees.add(employee(rs));
            }
        } catch (SQLException ignored) {
        }
        return employees;
    }

    private List<Employee> emWithManChain() {
        try {
            ResultSet rs = getRs("SELECT * FROM EMPLOYEE");
            List<Employee> dbEmployees = new LinkedList<>();
            while (rs.next()) {
                dbEmployees.add(empChain(rs));
            }
            return dbEmployees;
        } catch (SQLException ignored) {
        }
        return null;
    }

    private Employee employee(ResultSet rs) {
        try {
            Employee manager = null;
            if (rs.getString("MANAGER") != null) {
                manager = manager(rs, new BigInteger(rs.getString("MANAGER")));
            }
            return setEmployee(rs, manager);

        } catch (SQLException e) {
            return null;
        }
    }

    private Employee setEmployee(ResultSet rs, Employee employee) throws SQLException {
        BigInteger ID = new BigInteger(rs.getString("ID"));
        FullName name = new FullName(
                rs.getString("FIRSTNAME"),
                rs.getString("LASTNAME"),
                rs.getString("MIDDLENAME")
        );
        Position pos = Position.valueOf(rs.getString("POSITION"));
        LocalDate date = LocalDate.parse(rs.getString("HIREDATE"));
        BigDecimal sal = new BigDecimal(rs.getString("SALARY"));
        Department dep = null;
        if (rs.getString("DEPARTMENT") == null) {
            dep = null;
        } else {
            dep = dep(rs.getString("DEPARTMENT"));
        }
        return new Employee(
                ID,
                name,
                pos,
                date,
                sal,
                employee,
                dep);
    }

    private Employee manager(ResultSet rs, BigInteger id) {
        try {
            Employee manager = null;
            rs = getRs("SELECT * FROM EMPLOYEE");
            int p = rs.getRow();
            rs.first();
            rs.previous();
            while (rs.next()) {
                if (new BigInteger(rs.getString("ID")).equals(id)) {
                    manager = setEmployee(rs, null);
                    break;
                }
            }
            rs.absolute(p);
            return manager;
        } catch (SQLException execption) {
            return null;
        }
    }

    private Department dep(String id) {
//        if (id == null) return null;
        try {
            ResultSet rs = getRs("SELECT * FROM DEPARTMENT WHERE ID=" + id);
            if (rs.next())
                return new Department(
                        new BigInteger(rs.getString("ID")),
                        rs.getString("NAME"),
                        rs.getString("LOCATION")
                );
            else
                return null;
        } catch (SQLException execption) {
            return null;
        }
    }

    private Employee empChain(ResultSet rs) {
        try {
            Employee manager = null;
            if (rs.getString("MANAGER") != null) {
                manager = manWithChain(rs);
            }
            return setEmployee(rs, manager);
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> employeesWChain() {
        List<Employee> dbEmpls = new LinkedList<>();
        ResultSet rs = getRs("SELECT * FROM EMPLOYEE");
        try {
            while (rs.next()) {
                dbEmpls.add(empWMChain(rs));
            }
        } catch (SQLException ignored) {
        }
        return dbEmpls;
    }

    private Employee manWithChain(ResultSet rs) {
        try {
            int currentRowID = rs.getRow();
            int managerID = rs.getInt("manager");
            if (managerID == 0) return null;
            Employee manager = null;
            rs.beforeFirst();
            while (rs.next()) {
                if (rs.getInt("id") == managerID) {
                    manager = empWMChain(rs);
                    break;
                }
            }
            rs.absolute(currentRowID);
            return manager;
        } catch (SQLException e) {
            return null;
        }
    }

    private Employee empWMChain(ResultSet rs) {
        try {
            Employee manager = null;
            if (rs.getString("MANAGER") != null) {
                manager = manChain(rs);
            }
            return setEmployee(rs, manager);
        } catch (SQLException e) {
            return null;
        }
    }

    private Employee manChain(ResultSet rs) {
        try {
            int manID = rs.getInt("MANAGER");
            if (manID == 0) return null;
            int row = rs.getRow();
            Employee manager = null;
            rs.first();
            rs.previous();
            while (rs.next()) {
                if (rs.getInt("ID") == manID) {
                    manager = empWMChain(rs);
                    break;
                }
            }
            rs.absolute(row);
            return manager;
        } catch (SQLException e) {
            return null;
        }
    }

    private List<Employee> page(List<Employee> list, Paging paging) {
        return list.subList(paging.itemPerPage * (paging.page - 1), Math.min(paging.itemPerPage * paging.page, list.size()));
    }

    public EmployeeService employeeService() {
        return new EmployeeService() {
            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                List<Employee> employees = employee();
                List<Employee> sortedEmployees = new LinkedList<>(employees);
                sortedEmployees.sort(Comparator.comparing(Employee::getHired));
                return page(sortedEmployees, paging);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                List<Employee> employees = employee();
                List<Employee> sortedEmployees = new LinkedList<>(employees);
                Collections.sort(sortedEmployees, new Comparator<Employee>() {
                    @Override
                    public int compare(Employee o1, Employee o2) {
                        return o1.getFullName().getLastName().compareTo(o2.getFullName().getLastName());
                    }
                });
                return page(sortedEmployees, paging);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY SALARY");
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE ORDER BY DEPARTMENT, LASTNAME");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY HIREDATE");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY SALARY");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY LASTNAME");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY LASTNAME");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger managerID;
                        if (rs.getString("MANAGER") == null) {
                            managerID = null;
                        } else {
                            managerID = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, managerID));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY HIREDATE");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger managerID;
                        if (rs.getString("MANAGER") == null) {
                            managerID = null;
                        } else {
                            managerID = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, managerID));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE MANAGER=" + manager.getId() + " ORDER BY SALARY");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger managerID;
                        if (rs.getString("MANAGER") == null) {
                            managerID = null;
                        } else {
                            managerID = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, managerID));
                        list.add(employee);
                    }
                    return page(list, paging);
                } catch (SQLException expection) {
                    return null;
                }
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                List<Employee> list = employeesWChain();
                for (Employee emp : list) {
                    if (emp.getId().equals(employee.getId())) {
                        return emp;
                    }
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                try {
                    List<Employee> list = new LinkedList<>();
                    ResultSet rs = getRs("SELECT * FROM EMPLOYEE WHERE DEPARTMENT=" + department.getId() + " ORDER BY SALARY DESC");
                    assert rs != null;
                    while (rs.next()) {
                        BigInteger manager;
                        if (rs.getString("MANAGER") == null) {
                            manager = null;
                        } else {
                            manager = new BigInteger(rs.getString("MANAGER"));
                        }
                        Employee employee = setEmployee(rs, manager(rs, manager));
                        list.add(employee);
                    }
                    list.sort(
                            new Comparator<Employee>() {
                                @Override
                                public int compare(Employee o1, Employee o2) {
                                    return o1.getSalary().compareTo(o2.getSalary());
                                }
                            }
                                    .thenComparing((o1, o2) -> {
                                        if (o1.getId().compareTo(o2.getId()) < 0)
                                            return 1;
                                        else if (o1.getId().equals(o2.getId()))
                                            return 0;
                                        else
                                            return -1;
                                    })
                    );
                    List<Employee> reversed = new LinkedList<>();
                    for (int i = list.size() - 1; i > -1; i--) {
                        reversed.add(list.get(i));
                    }
                    return reversed.get(salaryRank - 1);
                } catch (SQLException expection) {
                    return null;
                }
            }
        };
    }
}
