// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period            time.Duration   `config:"period"`
	DBType            string   `yaml:"dbtype"`
	Hostname          string   `yaml:"hostname"`
	Port              string   `yaml:"port"`
	Username          string   `yaml:"username"`
	Password          string   `yaml:"password"`
	EncryptedPassword string   `yaml:"encryptedpassword"`
	Database          string   `yaml:"database"`
	PostgresSSLMode   string   `yaml:"postgressslmode"`
	Queries           []string `yaml:"queries"`
	QueryTypes        []string `yaml:"querytypes"`
	DeltaWildcard     string   `yaml:"deltawildcard"`
}

var DefaultConfig = Config{
	Period: 10 * time.Second,
	DBType: "",
	Hostname: "127.0.0.1",
	Username: "sqlbeat_user",
	Password: "sqlbeat_pass",
	DeltaWildcard: "__DELTA",
}
