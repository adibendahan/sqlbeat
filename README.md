# Sqlbeat
Fully customizable Beat for MySQL/Microsoft SQL Server/PostgreSQL servers - this beat can ship the results of any query defined on the config file to Elasticsearch.


## Current status
Sqlbeat still on beta.

#### To Do:
* Add SSPI support for MSSQL
* Add support for Oracle
* Add support for SQLite
* (Thinking about it) Add option to save connection string in the config file - will open support for all [SQLDrivers](https://github.com/golang/go/wiki/SQLDrivers).


## Features

* Connect to MySQL / Microsoft SQL Server / PostgreSQL and run queries
 * `single-row` queries will be translated as columnname:value.
 * `two-columns` will be translated as value-column1:value-column2 for each row.
 * `multiple-rows` each row will be a document (with columnname:value) - no DELTA support.
 * `show-slave-delay` will only send the "Seconds_Behind_Master" column from `SHOW SLAVE STATUS;` (For MySQL use)
* Any column that ends with the delatwildcard (default is __DELTA) will send delta results, extremely useful for server counters.
  `((newval - oldval)/timediff.Seconds())`

## How to Build

Sqlbeat uses Glide for dependency management. To install glide see: https://github.com/Masterminds/glide

```shell
$ glide update --no-recursive
$ make 
```

## Configuration

Edit mysqlbeat configuration in ```sqlbeat.yml``` .
You can:
 * Choose DB Type
 * Add queries to the `queries` array
 * Add query types to the `querytypes` array
 * Define Username/Password to connect to the DB server
 * Define the column wild card for delta columns
 * Password can be saved in clear text/AES encryption

Notes on password encryption: Before you compile your own mysqlbeat, you should put a new secret in the code (defined as a const), secret length must be 16, 24 or 32, corresponding to the AES-128, AES-192 or AES-256 algorithm. I recommend deleting the secret from the source code after you have your compiled mysqlbeat. You can encrypt your password with [mysqlbeat-password-encrypter](github.com/adibendahan/mysqlbeat-password-encrypter, "github.com/adibendahan/mysqlbeat-password-encrypter") just update your secret (and commonIV if you choose to change it) and compile.

## Template
 Since Sqlbeat runs custom queries only, a template can't be provided. Once you define the queries you should create your own template

## How to use
Just run ```sqlbeat -c sqlbeat.yml``` and you are good to go.

## License
GNU General Public License v2
