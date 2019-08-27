# dbipy
SQL Database Connector

#### Requires a jdbcConn.json file which looks like this:
```
"my_database_name":
    {
        "user":"userName",
        "passwd":"password",
        "jdbc_url":"jdbc:database_type://host/OPTION1=OPTION, DATABASE=DATEBASE_NAME ::PORT",
        "class_path":"com.database.jdbc.Driver",
        "jar_path":["/path/to/driver/database.jar"]
    }
}
```

### query_db
* Execute a query as a string

### executeOn_db
* Execute a .sql script 

### writeTo_db
* write a .csv to a table

### writeDfTo_db
* write a pandas dataframe to a table
