```python
!pip install mysql-connector-python
```

    Requirement already satisfied: mysql-connector-python in c:\users\nhart\anaconda3\lib\site-packages (8.3.0)
    


```python
import mysql.connector as connector
```


```python
connection = connector.connect(host = "localhost", user ="root", password = "Fairfield2024!")
```


```python
cursor=connection.cursor()
```


```python
cursor.execute("CREATE DATABASE little_lemon_dbv6")
```


```python
cursor.execute("USE little_lemon_dbv6")
```


```python
#MenuItems table
create_menuitem_table = """CREATE TABLE MenuItems (
ItemID INT AUTO_INCREMENT,
Name VARCHAR(200),
Type VARCHAR(100),
Price INT,
PRIMARY KEY (ItemID)
);"""
```


```python
create_menu_table = """CREATE TABLE Menus (
MenuID INT,
ItemID INT,
Cuisine VARCHAR(100),
PRIMARY KEY (MenuID,ItemID)
);"""
```


```python
create_booking_table = """CREATE TABLE Bookings (
BookingID INT AUTO_INCREMENT,
TableNo INT,
GuestFirstName VARCHAR(100) NOT NULL,
GuestLastName VARCHAR(100) NOT NULL,
BookingSlot TIME NOT NULL,
EmployeeID INT,
PRIMARY KEY (BookingID)
);"""
```


```python
create_orders_table = """CREATE TABLE Orders (
OrderID INT,
TableNo INT,
MenuID INT,
BookingID INT,
BillAmount INT,
Quantity INT,
PRIMARY KEY (OrderID,TableNo)
);"""
```


```python
create_employees_table = """CREATE TABLE Employees (
EmployeeID INT AUTO_INCREMENT PRIMARY KEY,
Name VARCHAR (255),
Role VARCHAR (100),
Address VARCHAR (255),
Contact_Number INT,
Email VARCHAR (255),
Annual_Salary VARCHAR (100)
);"""
```


```python
# Create MenuItems table
cursor.execute(create_menuitem_table)

# Create Menu table
cursor.execute(create_menu_table)

# Create Bookings table
cursor.execute(create_booking_table)

# Create Orders table
cursor.execute(create_orders_table)

# Create Employees table
cursor.execute(create_employees_table)
```


```python
#*******************************************************#
# Insert query to populate "MenuItems" table:
#*******************************************************#
insert_menuitems="""
INSERT INTO MenuItems (ItemID, Name, Type, Price)
VALUES
(1, 'Olives','Starters',5),
(2, 'Flatbread','Starters', 5),
(3, 'Minestrone', 'Starters', 8),
(4, 'Tomato bread','Starters', 8),
(5, 'Falafel', 'Starters', 7),
(6, 'Hummus', 'Starters', 5),
(7, 'Greek salad', 'Main Courses', 15),
(8, 'Bean soup', 'Main Courses', 12),
(9, 'Pizza', 'Main Courses', 15),
(10, 'Greek yoghurt','Desserts', 7),
(11, 'Ice cream', 'Desserts', 6),
(12, 'Cheesecake', 'Desserts', 4),
(13, 'Athens White wine', 'Drinks', 25),
(14, 'Corfu Red Wine', 'Drinks', 30),
(15, 'Turkish Coffee', 'Drinks', 10),
(16, 'Turkish Coffee', 'Drinks', 10),
(17, 'Kabasa', 'Main Courses', 17);"""

#*******************************************************#
# Insert query to populate "Menu" table:
#*******************************************************#
insert_menu="""
INSERT INTO Menus (MenuID,ItemID,Cuisine)
VALUES
(1, 1, 'Greek'),
(1, 7, 'Greek'),
(1, 10, 'Greek'),
(1, 13, 'Greek'),
(2, 3, 'Italian'),
(2, 9, 'Italian'),
(2, 12, 'Italian'),
(2, 15, 'Italian'),
(3, 5, 'Turkish'),
(3, 17, 'Turkish'),
(3, 11, 'Turkish'),
(3, 16, 'Turkish');"""

#*******************************************************#
# Insert query to populate "Bookings" table:
#*******************************************************#
insert_bookings="""
INSERT INTO Bookings (BookingID, TableNo, GuestFirstName, 
GuestLastName, BookingSlot, EmployeeID)
VALUES
(1, 12, 'Anna','Iversen','19:00:00',1),
(2, 12, 'Joakim', 'Iversen', '19:00:00', 1),
(3, 19, 'Vanessa', 'McCarthy', '15:00:00', 3),
(4, 15, 'Marcos', 'Romero', '17:30:00', 4),
(5, 5, 'Hiroki', 'Yamane', '18:30:00', 2),
(6, 8, 'Diana', 'Pinto', '20:00:00', 5);"""

#*******************************************************#
# Insert query to populate "Orders" table:
#*******************************************************#
insert_orders="""
INSERT INTO Orders (OrderID, TableNo, MenuID, BookingID, Quantity, BillAmount)
VALUES
(1, 12, 1, 1, 2, 86),
(2, 19, 2, 2, 1, 37),
(3, 15, 2, 3, 1, 37),
(4, 5, 3, 4, 1, 40),
(5, 8, 1, 5, 1, 43);"""

#*******************************************************#
# Insert query to populate "Employees" table:
#*******************************************************#
insert_employees = """
INSERT INTO employees VALUES
(01,'Mario Gollini','Manager','724, Parsley Lane, Old Town, Chicago, IL',351258074,'Mario.g@littlelemon.com','$70,000'),
(02,'Adrian Gollini','Assistant Manager','334, Dill Square, Lincoln Park, Chicago, IL',351474048,'Adrian.g@littlelemon.com','$65,000'),
(03,'Giorgos Dioudis','Head Chef','879 Sage Street, West Loop, Chicago, IL',351970582,'Giorgos.d@littlelemon.com','$50,000'),
(04,'Fatma Kaya','Assistant Chef','132  Bay Lane, Chicago, IL',351963569,'Fatma.k@littlelemon.com','$45,000'),
(05,'Elena Salvai','Head Waiter','989 Thyme Square, EdgeWater, Chicago, IL',351074198,'Elena.s@littlelemon.com','$40,000'),
(06,'John Millar','Receptionist','245 Dill Square, Lincoln Park, Chicago, IL',351584508,'John.m@littlelemon.com','$35,000');
"""

```


```python
# Populate MenuItems table
cursor.execute(insert_menuitems)
connection.commit()

# Populate MenuItems table
cursor.execute(insert_menu)
connection.commit()

# Populate Bookings table
cursor.execute(insert_bookings)
connection.commit()

# Populate Orders table
cursor.execute(insert_orders)
connection.commit()

# Populate Employees table
cursor.execute(insert_employees)
connection.commit()
```


```python
dbconfig = {"database": "little_lemon_dbv6", "user": "root", "password": "Fairfield2024!"}
```

Task 1:


```python
from mysql.connector import pooling, Error

# Create a connection pool named 'pool_b' with two connections
try:
    pool_b = pooling.MySQLConnectionPool(pool_name="pool_b", pool_size=2, **dbconfig)
    
    # Print success message
    print("Connection pool 'pool_b' created successfully.")

except Error as e:
    # Handle connection errors
    print("Error:", e)

```

    Connection pool 'pool_b' created successfully.
    

Task 2:


```python
from mysql.connector import Error

guests_data = [
    {"table_number": 8, "first_name": "Anees", "last_name": "Java", "booking_time": "18:00", "employee_id": 6},
    {"table_number": 5, "first_name": "Bald", "last_name": "Vin", "booking_time": "19:00", "employee_id": 6},
    {"table_number": 12, "first_name": "Jay", "last_name": "Kon", "booking_time": "19:30", "employee_id": 6}
]

try:
    # Get a connection from 'pool_b'
    connection = pool_b.get_connection()
    
    # Insert data for each guest
    cursor = connection.cursor()
    insert_query = "INSERT INTO Bookings (TableNo, GuestFirstName, GuestLastName, BookingSlot, EmployeeID) VALUES (%s, %s, %s, %s, %s)"
    for guest_data in guests_data:
        booking_data = (guest_data["table_number"], guest_data["first_name"], guest_data["last_name"], guest_data["booking_time"], guest_data["employee_id"])
        cursor.execute(insert_query, booking_data)
    connection.commit()
    cursor.close()
    
    # Print success message
    print("Bookings inserted successfully.")
    
    # Return the connection back to the pool
    connection.close()
    
except Error as e:
    # Handle connection errors
    print("Error:", e)

```

    Bookings inserted successfully.
    

Task 3:


```python
try:
    # Get a connection from 'pool_b'
    connection = pool_b.get_connection()
    cursor = connection.cursor()

    # Query 1: Little Lemon employee with title "Manager"
    query1 = "SELECT Name, EmployeeID FROM employees WHERE Role = 'Manager'"
    cursor.execute(query1)
    manager_info = cursor.fetchone()

    # Query 2: Employee with highest Annual_Salary
    query2 = "SELECT Name, Role FROM employees WHERE Annual_Salary = (SELECT MAX(Annual_Salary) FROM employees)"
    cursor.execute(query2)
    highest_salary_employee = cursor.fetchone()

    # Query 3: Count of guests booked between 18:00 and 20:00
    query3 = "SELECT COUNT(*) FROM bookings WHERE BookingSlot BETWEEN '18:00' AND '20:00'"
    cursor.execute(query3)
    guest_count = cursor.fetchone()[0]

    # Query 4: Concatenated GuestFirstName and GuestLastName and associated BookingID sorted by BookingSlot
    query4 = "SELECT CONCAT(GuestFirstName, ' ', GuestLastName) AS GuestName, BookingID FROM bookings ORDER BY BookingSlot"
    cursor.execute(query4)
    guest_info = cursor.fetchall()

    # Print the report
    print("Little Lemon Employee with Title Manager:", manager_info)
    print("Employee with Highest Annual Salary:", highest_salary_employee)
    print("Count of Guests Booked between 18:00 and 20:00:", guest_count)
    print("Guests Information Sorted by BookingSlot:")
    for guest in guest_info:
        print("Guest Name:", guest[0], "| BookingID:", guest[1])

    # Return the connection back to the pool
    connection.close()

except Error as e:
    # Handle connection errors
    print("Error:", e)

```

    Little Lemon Employee with Title Manager: ('Mario Gollini', 1)
    Employee with Highest Annual Salary: ('Mario Gollini', 'Manager')
    Count of Guests Booked between 18:00 and 20:00: 7
    Guests Information Sorted by BookingSlot:
    Guest Name: Vanessa McCarthy | BookingID: 3
    Guest Name: Marcos Romero | BookingID: 4
    Guest Name: Anees Java | BookingID: 7
    Guest Name: Hiroki Yamane | BookingID: 5
    Guest Name: Anna Iversen | BookingID: 1
    Guest Name: Joakim Iversen | BookingID: 2
    Guest Name: Bald Vin | BookingID: 8
    Guest Name: Jay Kon | BookingID: 9
    Guest Name: Diana Pinto | BookingID: 6
    

Task 4:


```python
import mysql.connector

# Establish a connection to the MySQL database
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Fairfield2024!",
    database="little_lemon_dbv6"
)

try:
    if connection.is_connected():
        # Create a cursor object
        cursor = connection.cursor()

        # Drop the stored procedure if it already exists
        drop_procedure_query = "DROP PROCEDURE IF EXISTS BasicSalesReport"
        cursor.execute(drop_procedure_query)
        connection.commit()

        # Define the SQL script to create the stored procedure
        sql_script = """
        CREATE PROCEDURE BasicSalesReport()
        BEGIN
            -- Total sales
            SELECT SUM(BillAmount) AS TotalSales FROM orders;

            -- Average sale
            SELECT AVG(BillAmount) AS AverageSale FROM orders;

            -- Minimum bill paid
            SELECT MIN(BillAmount) AS MinimumBillPaid FROM orders;

            -- Maximum bill paid
            SELECT MAX(BillAmount) AS MaximumBillPaid FROM orders;
        END
        """

        # Execute the SQL script to create the stored procedure
        cursor.execute(sql_script)

        # Commit the changes
        connection.commit()

        print("Stored procedure BasicSalesReport created successfully!")

        # Call the stored procedure BasicSalesReport
        cursor.callproc("BasicSalesReport")

        # Fetch the results
        for result in cursor.stored_results():
            print(result.fetchall())

        # Close the cursor
        cursor.close()

except mysql.connector.Error as error:
    # Handle any errors that may occur during the execution
    print("Error:", error)

    
finally:
    # Close the connection
    if 'connection' in locals():
        connection.close()
        print("Connection closed.")
```

    Stored procedure BasicSalesReport created successfully!
    [(Decimal('243'),)]
    [(Decimal('48.6000'),)]
    [(37,)]
    [(86,)]
    Connection closed.
    


```python
import mysql.connector

try:
    # Get a connection from 'pool_b'
    connection = pool_b.get_connection()
    
    # Create a buffered cursor
    cursor = connection.cursor(buffered=True)
    
    # Combine data from the bookings and employee tables
    query = """
    SELECT b.BookingSlot, CONCAT(b.GuestFirstName, ' ', b.GuestLastName) AS Guest_name, 
           CONCAT('Assigned to: ', e.Name, ', ', e.Role) AS Assignment
    FROM Bookings b
    JOIN Employees e ON b.EmployeeID = e.EmployeeID
    ORDER BY b.BookingSlot ASC
    LIMIT 3
    """
    
    # Execute the query
    cursor.execute(query)
    
    # Fetch the results
    results = cursor.fetchall()
    
    # Print the output
    for result in results:
        print("- [BookingSlot]:", result[0])
        print("- [Guest_name]:", result[1])
        print("- [Assignment]:", result[2])
        print()
    
except mysql.connector.Error as error:
    # Handle any errors that may occur during the execution
    print("Error:", error)

finally:
    # Return the connection back to the pool
    if 'connection' in locals():
        connection.close()
        print("Connection closed.")

```

    - [BookingSlot]: 15:00:00
    - [Guest_name]: Vanessa McCarthy
    - [Assignment]: Assigned to: Giorgos Dioudis, Head Chef
    
    - [BookingSlot]: 17:30:00
    - [Guest_name]: Marcos Romero
    - [Assignment]: Assigned to: Fatma Kaya, Assistant Chef
    
    - [BookingSlot]: 18:00:00
    - [Guest_name]: Anees Java
    - [Assignment]: Assigned to: John Millar, Receptionist
    
    Connection closed.
    


```python

```
