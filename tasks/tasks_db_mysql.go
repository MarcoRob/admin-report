package tasks

import (
	"database/sql"
	"fmt"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
)

const dbDoesNotExistError = 1049
const tableDoesNotExistError = 1146
const insertStatement = `
		INSERT INTO tasks_reports(
			completed_total, completed_on_time, completed_late, delayed_tasks,
				available_total, available_due_today
		)
		VALUES (?, ?, ?, ?, ?, ?);
	`
const getStatement = `
		SELECT *
		FROM tasks_reports
		WHERE report_id = ?;
	`
var createTableStatements = []string{
	`CREATE DATABASE IF NOT EXISTS arqui DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE arqui;`,
	`CREATE TABLE IF NOT EXISTS tasks_reports (
		report_id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		completed_total INT UNSIGNED,
		completed_on_time INT UNSIGNED,
		completed_late INT UNSIGNED,
		delayed_tasks INT UNSIGNED,
		available_total INT UNSIGNED,
		available_due_today INT UNSIGNED
	);`,
}

type mysqlDB struct {
	conn *sql.DB

	insert 		*sql.Stmt
	get			*sql.Stmt
}

var _ TasksReportDatabase = &mysqlDB{}

type MySQLConfig struct {
	Username, Password 	string
	Host 				string
	Port 				int
}

// return connection string for sql.Open
func (c MySQLConfig) dataStoreName(dbName string) string {
	var credentials string
	if c.Username != "" {
		credentials = c.Username
		if c.Password != "" {
			credentials = credentials + ":" + c.Password
		}
		credentials = credentials + "@"
	}
	return fmt.Sprintf("%stcp([%s]:%d)/%s", credentials, c.Host, c.Port, dbName)
}

func newMySQLDB(config MySQLConfig) (TasksReportDatabase, error) {
	if err := config.ensureTableExists(); err != nil {
		return nil, err
	}

	conn, err := sql.Open("mysql", config.dataStoreName("arqui"))
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("mysql: could not establish a good connection: %v", err)
	}

	db := &mysqlDB {
		conn: conn,
	}

	if db.get, err = conn.Prepare(getStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare get: %v", err)
	}
	if db.insert, err = conn.Prepare(insertStatement); err != nil {
		return nil, fmt.Errorf("mysql: prepare insert: %v", err)
	}

	return db, nil
}

func (db *mysqlDB) Close() {
	db.conn.Close()
}

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanTasksReport(s rowScanner) (*TasksReport, error) {
	var (
		reportId			int64
		completedTotal		int
		completedOnTime 	int
		completedLate		int
		delayed				int
		availableTotal		int
		availableDueToday	int
	)
	if err := s.Scan(&reportId, &completedTotal, &completedOnTime, &completedLate,
		&delayed, &availableTotal, &availableDueToday); err != nil {
		return nil, err
	}

	report := &TasksReport{
		ReportID:reportId,
		Completed:CompletedDescription{completedTotal, completedOnTime,
			completedLate},
		Delayed:delayed,
		Available:AvailableDescription{availableTotal, availableDueToday},
	}
	return report, nil
}

// if table doesn't exist, create it
func (config MySQLConfig) ensureTableExists() error {
	conn, err := sql.Open("mysql", config.dataStoreName(""))
	if err != nil {
		return fmt.Errorf("mysql: could not get a connection: %v", err)
	}
	defer conn.Close()

	if conn.Ping() == driver.ErrBadConn {
		return fmt.Errorf("mysql: could not connect to db. ")
	}

	if _, err := conn.Exec(`USE arqui`); err != nil {
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == dbDoesNotExistError {
			return createTable(conn)
		}
	}

	if _, err := conn.Exec(`DESCRIBE tasks_reports`); err != nil {
		if mErr, ok := err.(*mysql.MySQLError); ok && mErr.Number == tableDoesNotExistError {
			return createTable(conn)
		}
		return fmt.Errorf("mysql: could not connect to db: %v", err)
	}
	return nil
}

// create db and table, as necessary
func createTable(conn *sql.DB) error {
	for _, stmt := range createTableStatements {
		_, err := conn.Exec(stmt)
		if err != nil {
			return err
		}
	}
	return nil
}

// execute a statement, expecting one row affected
func execAffectingOneRow(stmt *sql.Stmt, args ...interface{}) (sql.Result, error) {
	result, err := stmt.Exec(args...)
	if err != nil {
		return result, fmt.Errorf("mysql: could not execute statement: %v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return result, fmt.Errorf("mysql: could not get rows affected: %v", err)
	} else if rowsAffected != 1 {
		return result, fmt.Errorf("mysql: expected 1 row affected, got %d", rowsAffected)
	}
	return result, nil
}

func (db *mysqlDB) GetTasksReport(reportId int64) (*TasksReport, error) {
	report, err := scanTasksReport(db.get.QueryRow(reportId))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("msql: could not find report with id %d", reportId)
	}
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get tasks report: %v", err)
	}
	return report, nil
}

func (db *mysqlDB) AddTasksReport(report *TasksReport) (reportId int64, err error) {
	result, err := execAffectingOneRow(db.insert, report.Completed.Total,
		report.Completed.OnTime, report.Completed.Late, report.Delayed,
		report.Available.Total, report.Available.DueToday)
	if err != nil {
		return 0, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("mysql: could not get last insert ID: %v", err)
	}

	return lastInsertID, nil
}