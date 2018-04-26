package habits

import (
	"database/sql"
	"fmt"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
)

const dbDoesNotExistError = 1049
const tableDoesNotExistError = 1146
const insertStatement = `
		INSERT INTO habits_reports(
			red, orange, yellow, green, blue, worst_name,
				worst_title, best_name, best_title
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
	`
const getStatement = `
		SELECT *
		FROM habits_reports
		WHERE report_id = ?;
	`
var createTableStatements = []string{
	`CREATE DATABASE IF NOT EXISTS arqui DEFAULT CHARACTER SET = 'utf8' DEFAULT COLLATE 'utf8_general_ci';`,
	`USE arqui;`,
	`CREATE TABLE IF NOT EXISTS habits_reports (
		report_id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
		red INT UNSIGNED,
		orange INT UNSIGNED,
		yellow INT UNSIGNED,
		green INT UNSIGNED,
		blue INT UNSIGNED,
		worst_name TEXT,
		worst_title TEXT,
		best_name TEXT,
		best_title TEXT
	);`,
}

type mysqlDB struct {
	conn *sql.DB

	insert 		*sql.Stmt
	get			*sql.Stmt
}

var _ HabitsReportDatabase = &mysqlDB{}

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

func newMySQLDB(config MySQLConfig) (HabitsReportDatabase, error) {
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

func scanHabitsReport(s rowScanner) (*HabitsReport, error) {
	var (
		reportId		int64
		red				int
		orange			int
		yellow			int
		green			int
		blue			int
		worstName		sql.NullString
		worstTitle		sql.NullString
		bestName		sql.NullString
		bestTitle		sql.NullString
	)
	if err := s.Scan(&reportId, &red, &orange, &yellow, &green,
		&blue, &worstName, &worstTitle, &bestName, &bestTitle); err != nil {
		return nil, err
	}

	report := &HabitsReport{
		ReportID:reportId,
		RangeCount:HabitRange{red,orange,yellow,
		green,blue},
		Worst:HabitDescription{worstName.String, worstTitle.String},
		Best:HabitDescription{bestName.String, bestTitle.String},
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

	if _, err := conn.Exec(`DESCRIBE habits_reports`); err != nil {
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

func (db *mysqlDB) GetHabitsReport(reportId int64) (*HabitsReport, error) {
	report, err := scanHabitsReport(db.get.QueryRow(reportId))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("msql: could not find report with id %d", reportId)
	}
	if err != nil {
		return nil, fmt.Errorf("mysql: could not get habits report: %v", err)
	}
	return report, nil
}

func (db *mysqlDB) AddHabitsReport(report *HabitsReport) (reportId int64, err error) {
	result, err := execAffectingOneRow(db.insert, report.RangeCount.Red,
		report.RangeCount.Orange, report.RangeCount.Yellow,
		report.RangeCount.Green, report.RangeCount.Blue,
		report.Worst.User, report.Worst.Title, report.Best.User,
		report.Best.Title)
	if err != nil {
		return 0, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("mysql: could not get last insert ID: %v", err)
	}

	return lastInsertID, nil
}