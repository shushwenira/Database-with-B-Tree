# Database with B+ Tree
A rudimentary database designed in Java using B+ Trees

- Compile the project source file 
		- "javac DavisBaseLite.java"
- Run the project using following command
		- "java DavisBaseLite"
		  (davisql> prompt will be started.)


Design Decisions:

- Every command literal (keywords, values, column names, commas, brackets, conditions, operators, etc.) must be space separated. Otherwise it will throw a syntax error.
E.g.: CREATE TABLE employee ( rowid INT PRIMARY KEY , name TEXT NOT NULL );

- ‘rowid’ must be specified in the create command and should be of INT data type, should be the first column listed and have only PRIMARY KEY constraint. It is used as a table index while creating and traversing B+ trees. 

- ‘rowid’ must not be specified in the insert command. It is generated in the background.

- TEXT values should not be updated with length more than what was first inserted. 
E.g.: If a value of a TEXT column was first inserted as ‘abcd’, updates on this value can be of 0 (NULL, empty strings do not exist in DavisBaseLite) to 4 chars long. Updating this value with a string of 5 chars long or more will result in corruption of data. 
Important: If the first insert to a TEXT data type column is a NULL value; It should remain NULL throughout. Otherwise data corruption may occur.

- DROP TABLE <table_name>; functionality is implemented by making use of the ‘is_active’ value in the davisabse_tables and davisbase_columns meta-data tables. Hence, the SHOW TABLES; command will return tables that are active. It is equivalent to following command: SELECT table_name FROM davisbase_tables WHERE is_active != 0;

- A column can have NULL values by not specifying any constraints in create command.
E.g.: CREATE TABLE employee ( rowid INT PRIMARY KEY , name TEXT NOT NULL , age INT ); 
age value can be NULL in above table data.

- String values with spaces are not supported.
E.g.: Following query is not supported: INSERT INTO employee VALUES ( ‘John Doe’ );
Instead use “_”: INSERT INTO employee VALUES ( ‘John_Doe’ );


Software stack used while developing and running the project	
- Language: Java 9
- Compiler: jdk-9.0.4
- IDE: Eclipse JEE Oxygen
- O/S: Windows 10
