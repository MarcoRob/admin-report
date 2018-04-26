package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"fmt"
	"github/godspeedkil/admin-report/habits"
	"strconv"
	"encoding/json"
	"github/godspeedkil/admin-report/tasks"
)

const (
	DECIMAL_BASE = 10
	INT64_BITS = 64
)

func main() {
	registerHandlers()
}

func registerHandlers() {
	router := mux.NewRouter()

	router.Methods("GET").Path("/admin/habits/reports").
		Handler(appHandler(createHabitsReportHandler))
	router.Methods("GET").Path("/admin/habits/reports/{reportId}").
		Handler(appHandler(getHabitsReportHandler))
	router.Methods("GET").Path("/admin/tasks/reports").
		Handler(appHandler(createTasksReportHandler))
	router.Methods("GET").Path("/admin/tasks/reports/{reportId}").
		Handler(appHandler(getTasksReportHandler))

	log.Fatal(http.ListenAndServe(":8001", router))
}

func getHabitsReportHandler(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	vars := mux.Vars(r)
	reportId, err := strconv.ParseInt(vars["reportId"], DECIMAL_BASE, INT64_BITS)
	if err != nil {
		return appErrorf(err, "could not parse reportId from request: %v", err)
	}
	report, err := habits.DB.GetHabitsReport(reportId)
	if err != nil {
		return appErrorf(err, "could not get report: %v", err)
	}
	json.NewEncoder(w).Encode(report)
	return nil
}

func createHabitsReportHandler(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")

	report, err := habits.GenerateHabitsReport()
	if err != nil {
		return appErrorf(err, "could not generate habits report: %v", err)
	}

	reportId, err := habits.DB.AddHabitsReport(&report)
	if err != nil {
		return appErrorf(err, "could not add report to db: %v", err)
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/habits/reports/%d", reportId),
		http.StatusFound)
	return nil
}

func getTasksReportHandler(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	vars := mux.Vars(r)
	reportId, err := strconv.ParseInt(vars["reportId"], DECIMAL_BASE, INT64_BITS)
	if err != nil {
		return appErrorf(err, "could not parse reportId from request: %v", err)
	}
	report, err := tasks.DB.GetTasksReport(reportId)
	if err != nil {
		return appErrorf(err, "could not get report: %v", err)
	}
	json.NewEncoder(w).Encode(report)
	return nil
}

func createTasksReportHandler(w http.ResponseWriter, r *http.Request) *appError {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")

	report, err := tasks.GenerateTasksReport()
	if err != nil {
		return appErrorf(err, "could not generate tasks report: %v", err)
	}

	reportId, err := tasks.DB.AddTasksReport(&report)
	if err != nil {
		return appErrorf(err, "could not add report to db: %v", err)
	}
	http.Redirect(w, r, fmt.Sprintf("/admin/tasks/reports/%d", reportId),
		http.StatusFound)
	return nil
}

type appHandler func(http.ResponseWriter, *http.Request) *appError

type appError struct {
	Error	error
	Message	string
	Code	int
}

func (fn appHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	if e := fn(w, r); e != nil {
		log.Printf("Handler error: status code: %d, message: %s, underlying err: %#v",
			e.Code, e.Message, e.Error)

		http.Error(w, e.Message, e.Code)
	}
}

func appErrorf(err error, format string, v ...interface{}) *appError {
	return &appError{
		Error:   err,
		Message: fmt.Sprintf(format, v...),
		Code:    500,
	}
}