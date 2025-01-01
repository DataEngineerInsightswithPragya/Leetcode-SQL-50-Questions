-- Input:
--Employees table:
--+----+----------+
--| id | name     |
--+----+----------+
--| 1  | Alice    |
--| 7  | Bob      |
--| 11 | Meir     |
--| 90 | Winston  |
--| 3  | Jonathan |
--+----+----------+
--EmployeeUNI table:
--+----+-----------+
--| id | unique_id |
--+----+-----------+
--| 3  | 1         |
--| 11 | 2         |
--| 90 | 3         |
--+----+-----------+
--Output:
--+-----------+----------+
--| unique_id | name     |
--+-----------+----------+
--| null      | Alice    |
--| null      | Bob      |
--| 2         | Meir     |
--| 3         | Winston  |
--| 1         | Jonathan |
--+-----------+----------+

select
    eu.unique_id,
    e.name
from employees e left join employeeuni eu
on e.id = eu.id
