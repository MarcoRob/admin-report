package tasks

import "log"

var DB TasksReportDatabase

func init() {
	var err error

	DB, err = configureCloudSQL(cloudSQLConfig{
		Username: "root",
		Password: "admin",
	})

	if err != nil {
		log.Fatal(err)
	}
}

type cloudSQLConfig struct {
	Username, Password string
}

func configureCloudSQL(config cloudSQLConfig) (TasksReportDatabase, error) {
	return newMySQLDB(MySQLConfig{
		Username: 	config.Username,
		Password: 	config.Password,
		Host:		"localhost",
		Port:		3306,
	})
}