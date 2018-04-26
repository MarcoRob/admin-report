package habits

import (
	"net/http"
	"errors"
	"encoding/json"
	"fmt"
)

const (
	//HABITS_URL = "https://api.myjson.com/bins/1end73"
	HABITS_URL = "https://habits-microservice-marcorob.c9users.io"
	//ACCOUNTS_URL = "http..."
	COLOR_RED = "red darken-1"
	COLOR_ORANGE = "orange darken-1"
	COLOR_YELLOW = "yellow darken-2"
	COLOR_GREEN = "light-green darken-1"
	COLOR_BLUE = "blue darken-1"
)

type Habit struct {
	Difficulty 	string 	`json:"difficulty"`
	Color		string	`json:"color"`
	Score 		int 	`json:"score"`
	HabitID		string	`json:"_id"`
	UserID		string	`json:"userID"`
	Type 		string 	`json:"type"`
	Title		string	`json:"title"`
}

type HabitDescription struct {
	User		string	`json:"user"`
	Title		string	`json:"title"`
}

type HabitRange struct {
	Red		int	`json:"red"`
	Orange	int	`json:"orange"`
	Yellow	int	`json:"yellow"`
	Green	int	`json:"green"`
	Blue	int	`json:"blue"`
}

type HabitsReport struct {
	ReportID		int64				`json:"reportID"`
	RangeCount 		HabitRange			`json:"rangeCount"`
	Worst 			HabitDescription	`json:"worst"`
	Best	 		HabitDescription	`json:"best"`
}

type HabitsReportDatabase interface {
	AddHabitsReport(*HabitsReport) (reportId int64, err error)

	GetHabitsReport(reportId int64)	(*HabitsReport, error)

	Close()
}

func fetchDatabaseAllHabits() ([]Habit, error) {
	resp, err := http.Get(HABITS_URL + "/habits")
	if err != nil {
		return []Habit{}, errors.New("Habits unavailable")
	}

	jsonArray := make([]Habit, 0)
	decoder := json.NewDecoder(resp.Body)
	defer resp.Body.Close()
	if err = decoder.Decode(&jsonArray); err != nil {
		return []Habit{}, errors.New("Error decoding Habits")
	}

	fmt.Println(jsonArray)

	return jsonArray, nil
}

func createHabitRange(allHabits []Habit) HabitRange {
	var habitRange HabitRange
	for i, _ := range allHabits {
		if allHabits[i].Color == COLOR_RED {
			habitRange.Red++
		} else if allHabits[i].Color == COLOR_ORANGE {
			habitRange.Orange++
		} else if allHabits[i].Color == COLOR_YELLOW {
			habitRange.Yellow++
		} else if allHabits[i].Color == COLOR_GREEN {
			habitRange.Green++
		} else if allHabits[i].Color == COLOR_BLUE {
			habitRange.Blue++
		}
	}
	return habitRange
}

func findWorstHabit(allHabits []Habit) HabitDescription {
	currentWorst := allHabits[0]
	for i, _ := range allHabits {
		if allHabits[i].Score < currentWorst.Score {
			currentWorst = allHabits[i]
		}
	}

	return HabitDescription{currentWorst.UserID, currentWorst.Title}
}

func findBestHabit(allHabits []Habit) HabitDescription {
	currentBest := allHabits[0]
	for i, _ := range allHabits {
		if allHabits[i].Score > currentBest.Score {
			currentBest = allHabits[i]
		}
	}

	return HabitDescription{currentBest.UserID, currentBest.Title}
}

func GenerateHabitsReport() (HabitsReport, error) {
	var habitsReport HabitsReport
	allHabits, err := fetchDatabaseAllHabits()
	if err != nil {
		return habitsReport, err
	}

	habitsReport.RangeCount = createHabitRange(allHabits)
	habitsReport.Worst = findWorstHabit(allHabits)
	habitsReport.Best = findBestHabit(allHabits)

	return habitsReport, nil
}