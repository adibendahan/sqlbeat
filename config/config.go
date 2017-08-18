// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period            time.Duration   `config:"period"`
	DBType            string   `config:"dbtype"`
	Hostname          string   `config:"hostname"`
	Port              string   `config:"port"`
	Username          string   `config:"username"`
	Password          string   `config:"password"`
	EncryptedPassword string   `config:"encryptedpassword"`
	Database          string   `config:"database"`
	PostgresSSLMode   string   `config:"postgressslmode"`
	Queries           []string `config:"queries"`
	QueryTypes        []string `config:"querytypes"`
	DeltaWildcard     string   `config:"deltawildcard"`
}

var DefaultConfig = Config{
	Period: 10 * time.Second,
	DBType: "",
	Hostname: "127.0.0.1",
	Username: "sqlbeat_user",
	Password: "sqlbeat_pass",
	Database: "",
	PostgresSSLMode: "disable",
	DeltaWildcard: "__DELTA",
}
