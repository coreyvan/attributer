package main

import (
	"encoding/csv"
	"go.uber.org/zap"
	"os"
	"sort"
	"strconv"
	"time"
)

func main() {
	log, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	if err := run(log); err != nil {
		log.Sugar().Fatalf("error running: %v", err)
	}
}

type Record struct {
	UserID    string
	Timestamp time.Time
	Type      string
	Value     string
}

type Dimension struct {
	Value      string
	Purchasers map[string]struct{}
	TotalSales float64
}

func run(log *zap.Logger) error {
	var records []Record

	//	parse impressions csv into timeline
	impressions, err := ParseFileIntoRecords("ad_exposures.csv", "exposure")
	if err != nil {
		return err
	}

	//	parse sales csv into timeline
	sales, err := ParseFileIntoRecords("sales_data.csv", "sale")
	if err != nil {
		return err
	}

	records = append(records, impressions...)
	records = append(records, sales...)

	// sort by timestamp
	sort.Slice(records, func(i, j int) bool {
		return records[j].Timestamp.After(records[i].Timestamp)
	})

	// run through list, track latest impression for each user
	summary := make(map[string]Dimension)
	overall := Dimension{
		Value:      "overall",
		Purchasers: make(map[string]struct{}),
		TotalSales: 0,
	}

	var noAdSalesCount int
	userLatestExposures := make(map[string]string)
	for _, r := range records {
		if r.Type == "exposure" {
			userLatestExposures[r.UserID] = r.Value
			continue
		}

		saleVal, err := strconv.ParseFloat(r.Value, 64)
		if err != nil {
			return err
		}

		impression, ok := userLatestExposures[r.UserID]
		if !ok {
			impression = ""
		}

		if impression == "" {
			noAdSalesCount++
			continue
		}

		overall.TotalSales += saleVal
		overall.Purchasers[r.UserID] = struct{}{}

		dimension, ok := summary[impression]
		if !ok {
			dimension = Dimension{
				Value:      impression,
				Purchasers: make(map[string]struct{}),
				TotalSales: 0,
			}
		}

		dimension.Purchasers[r.UserID] = struct{}{}
		dimension.TotalSales += saleVal

		summary[impression] = dimension
	}

	var out [][]string
	out = append(out, []string{"dimension", "value", "num_purchases", "total_sales"})
	out = append(out, []string{"overall", "overall", strconv.Itoa(len(overall.Purchasers)), strconv.FormatFloat(overall.TotalSales, 'f', 2, 64)})

	for k, d := range summary {
		out = append(out, []string{k, k, strconv.Itoa(len(d.Purchasers)), strconv.FormatFloat(d.TotalSales, 'f', 2, 64)})
	}

	sumFile, err := os.Create("summary.csv")
	if err != nil {
		return err
	}
	defer sumFile.Close()

	return csv.NewWriter(sumFile).WriteAll(out)
}

func ParseCSV(filename string) ([][]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	lines, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}

	return lines[1:], nil
}

func ParseFileIntoRecords(filename string, recordType string) ([]Record, error) {
	lines, err := ParseCSV(filename)
	if err != nil {
		return nil, err
	}

	records := make([]Record, 0, len(lines))
	for _, l := range lines {
		ts, err := time.Parse("2006-01-02 15:04:05", l[1])
		if err != nil {
			return nil, err
		}

		r := Record{
			UserID:    l[0],
			Timestamp: ts,
			Type:      recordType,
			Value:     l[2],
		}

		records = append(records, r)
	}

	return records, nil
}
