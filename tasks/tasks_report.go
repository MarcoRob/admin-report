package tasks

import (
	"net/http"
	"errors"
	"encoding/json"
	"time"
)

const (
	//TASKS_URL = "https://api.myjson.com/bins/6pkr3"
	TASKS_URL = "http://10.43.88.167:8080"
)

type Task struct {
	CompletedDate 	*int64		`json:"completedDate"`
	Description 	string 		`json:"description"`
	DueDate 		int64 		`json:"dueDate"`
	Reminder 		int64		`json:"remind"`
	Title 			string 		`json:"title"`
	UserID			string		`json:"userId"`
}

type CompletedDescription struct {
	Total	int	`json:"total"`
	OnTime	int	`json:"onTime"`
	Late	int	`json:"late"`
}

type AvailableDescription struct {
	Total		int	`json:"total"`
	DueToday	int	`json:"dueToday"`
}

type TasksReport struct {
	ReportID		int64					`json:"reportID"`
	Completed		CompletedDescription	`json:"completed"`
	Delayed			int						`json:"delayed"`
	Available		AvailableDescription	`json:"available"`
}

type TasksReportDatabase interface {
	AddTasksReport(*TasksReport) (reportId int64, err error)

	GetTasksReport(reportId int64)	(*TasksReport, error)

	Close()
}

func fetchDatabaseAllTasks() ([]Task, error) {
	resp, err := http.Get(TASKS_URL + "/Task/tasks")
	if err != nil {
		return []Task{}, errors.New("Tasks unavailable")
	}

	jsonArray := make([]Task, 0)
	decoder := json.NewDecoder(resp.Body)
	defer resp.Body.Close()
	if err = decoder.Decode(&jsonArray); err != nil {
		return []Task{}, errors.New("Error decoding Tasks")
	}

	return jsonArray, nil
}

func populateCompleted(allTasks []Task) CompletedDescription {
	var completed CompletedDescription
	for i, _ := range allTasks {
		if allTasks[i].CompletedDate == nil {
			continue
		}
		if *allTasks[i].CompletedDate <= allTasks[i].DueDate {
			completed.Total++
			completed.OnTime++
		} else if *allTasks[i].CompletedDate > allTasks[i].DueDate {
			completed.Total++
			completed.Late++
		}
	}

	return completed
}

func countDelayed(allTasks []Task) int {
	var delayedCount int
	for i, _ := range allTasks {
		if allTasks[i].CompletedDate == nil && allTasks[i].DueDate < time.Now().Unix() {
			delayedCount++
		}
	}

	return delayedCount
}

func populateAvailable(allTasks []Task) AvailableDescription {
	var available AvailableDescription
	for i,_ := range allTasks {
		if allTasks[i].CompletedDate == nil {
			available.Total++
			if time.Unix(allTasks[i].DueDate, 0).YearDay() == time.Now().YearDay() {
				available.DueToday++
			}
		}
	}

	return available
}

func GenerateTasksReport() (TasksReport, error) {
	var tasksReport TasksReport
	allTasks, err := fetchDatabaseAllTasks()
	if err != nil {
		return tasksReport, err
	}

	tasksReport.Completed = populateCompleted(allTasks)
	tasksReport.Delayed = countDelayed(allTasks)
	tasksReport.Available = populateAvailable(allTasks)

	return tasksReport, nil
}