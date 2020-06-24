package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/sensu-community/sensu-plugin-sdk/sensu"
	corev2 "github.com/sensu/sensu-go/api/core/v2"
	"os"
)

type HandlerConfig struct {
	sensu.PluginConfig
	dsn      string
	user     string
	password string
	schema   string
	table    string
}

var (
	config = HandlerConfig{
		PluginConfig: sensu.PluginConfig{
			Name:     "sensu-clickhouse-handler",
			Short:    "The Sensu Go Clickhouse handler",
			Keyspace: "github.com/akozlenkov/sensu-clickhouse-handler",
		},
	}

	configOptions = []*sensu.PluginConfigOption{
		{
			Path:      "dsn",
			Env:       "CLICKHOUSE_DSN",
			Argument:  "dsn",
			Shorthand: "d",
			Usage:     "The clickhouse dsn",
			Value:     &config.dsn,
		},
		{
			Path:      "table",
			Env:       "CLICKHOUSE_TABLE",
			Argument:  "table",
			Shorthand: "t",
			Usage:     "The clickhouse table",
			Value:     &config.dsn,
		},
	}
)

func main() {
	goHandler := sensu.NewGoHandler(&config.PluginConfig, configOptions, checkArgs, sendMessage)
	goHandler.Execute()
}

func checkArgs(_ *corev2.Event) error {
	if dsn := os.Getenv("CLICKHOUSE_DSN"); dsn != "" {
		config.dsn = dsn
	}

	if table := os.Getenv("CLICKHOUSE_TABLE"); table != "" {
		config.table = table
	}

	if len(config.dsn) == 0 {
		return fmt.Errorf("--dsn or CLICKHOUSE_DSN environment variable is required")
	}

	if len(config.table) == 0 {
		return fmt.Errorf("--table or CLICKHOUSE_TABLE environment variable is required")
	}

	return nil
}

func sendMessage(event *corev2.Event) error {
	connect, err := sql.Open("clickhouse", config.dsn)
	if err != nil {
		return err
	}

	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare(fmt.Sprintf("INSERT INTO %s (ts, metric, value, tags) VALUES (?, ?, ?, ?)", config.table))
	)

	defer stmt.Close()

	for _, m := range event.Metrics.Points {
		tags, err := json.Marshal(m.Tags)
		if err != nil {
			return err
		}

		if _, err := stmt.Exec(m.Timestamp, m.Name, m.Value, string(tags)); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}
