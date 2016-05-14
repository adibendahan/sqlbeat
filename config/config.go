// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

type Config struct {
	Sqlbeat SqlbeatConfig
}

type SqlbeatConfig struct {
	Period            string   `yaml:"period"`
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
