package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

const (
	MAXRECORDS = 1000

	INSERT = `INSERT INTO [Clickstream].[dbo].[%s]
           ([timestamp]
           ,[received]
           ,[deviceId]
           ,[eventCode]
           ,[msoName])
     VALUES 
	`
)

func ReadSvcFile(cvsFile string) [][]string {
	file, err := os.Open(cvsFile)

	if err != nil {
		fmt.Println("Error opening file: ", err)
		fmt.Println("File: ", cvsFile)
		os.Exit(-1)
	}

	r := csv.NewReader(file)

	records, er := r.ReadAll()
	if er != nil {
		log.Fatal(er)
	}

	return records
}

var (
	userid     string
	password   string
	server     string
	database   string
	table      string
	cvsFile    string
	maxRecords int
	silent     bool
)

func init() {
	flaguserid := flag.String("U", "", "`login`_id")
	flagpassword := flag.String("P", "", "`password`")
	flagserver := flag.String("S", "", "`server_name`[\\instance_name]")
	flagdatabase := flag.String("d", "", "`db_name`")
	flagtableName := flag.String("t", "clickstreamEventsLog", "`Table` name")
	flagcvsFile := flag.String("I", "", "`CVS` file path/name")
	flagmaxRecords := flag.Int("m", MAXRECORDS, "`How many` to insert at once")
	flagSilent := flag.Bool("s", true, "`Silent` execution")

	flag.Parse()

	if flag.Parsed() {
		userid = *flaguserid
		password = *flagpassword
		server = *flagserver
		database = *flagdatabase
		table = *flagtableName
		cvsFile = *flagcvsFile
		maxRecords = *flagmaxRecords
		silent = *flagSilent
	} else {
		flag.Usage()
		os.Exit(-1)
	}

	if !silent {
		fmt.Println("userid: ", userid)
		fmt.Println("password: ", password)
		fmt.Println("server: ", server)
		fmt.Println("database: ", database)
		fmt.Println("cvsFile: ", cvsFile)
		fmt.Println("maxRecords: ", maxRecords)
	}

	if (database == "") || (server == "") || (cvsFile == "") {
		fmt.Println("Need server, database, and filename")
		flag.Usage()
		os.Exit(-1)
	}

}

type SqlStatement struct {
	no  int
	sql string
}

func main() {
	records := ReadSvcFile(cvsFile)

	fmt.Printf("Read %v records total\n", len(records))

	dsn := "server=" + server + ";user id=" + userid + ";password=" + password + ";database=" + database
	db, err := sql.Open("mssql", dsn)
	if err != nil {
		fmt.Println("Cannot connect: ", err.Error())
		os.Exit(-1)
	}
	err = db.Ping()
	if err != nil {
		fmt.Println("Cannot connect: ", err.Error())
		os.Exit(-1)
	}
	defer db.Close()

	fmt.Printf("Succesffully connected to %v - %v DB\n", server, database)

	executeStatements(prepareStatements(records), db)

	fmt.Println("Completed processing: ", cvsFile)
}

func executeStatements(inChan <-chan SqlStatement, db *sql.DB) {

	for {
		if sql, next := <-inChan; next {
			if !silent {
				fmt.Printf("About to execute: %v...\n", sql.sql[:100])
			}
			err := exec(db, sql.sql)
			if err != nil {
				fmt.Printf("Error on executing query #%v for %v\n", sql.no, sql.sql)
				fmt.Println("Message: ", err)
			} else {
				if !silent {
					fmt.Println("Success.. #", sql.no)
				}
			}
		} else {
			if !silent {
				fmt.Println("No more statements for: ", cvsFile)
			}
			return
		}
	}
}

func prepareStatements(records [][]string) chan SqlStatement {
	//var statementsToExecute []string
	var valuesString, sqlStatement string
	ch := make(chan SqlStatement)

	headSql := fmt.Sprintf(INSERT, table)

	go func() {
		lastNo := 0
		for i, record := range records {
			if (i+1)%maxRecords == 0 {
				sqlStatement = headSql + valuesString[1:len(valuesString)]
				//statementsToExecute = append(statementsToExecute, sqlStatement)
				ch <- SqlStatement{lastNo, sqlStatement}
				lastNo++

				if !silent {
					fmt.Println("Generated: ", sqlStatement)
					fmt.Println("--------------------------------")
				}
				valuesString = ""
			}
			valuesString = valuesString + fmt.Sprintf(", ( '%s', '%s', '%s', '%s', '%s') ",
				// 2016-05-10 17:14:30
				record[0][:19], strings.Replace(record[1][1:], "_", " ", -1), record[2][1:], record[3][1:], record[4][1:])
		}

		sqlStatement = headSql + valuesString[1:len(valuesString)]
		//statementsToExecute = append(statementsToExecute, sqlStatement)
		ch <- SqlStatement{lastNo, sqlStatement}
		close(ch)
	}()

	return ch
}

func exec(db *sql.DB, cmd string) error {
	rows, err := db.Query(cmd)
	if err != nil {
		return err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if cols == nil {
		return nil
	}
	vals := make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		vals[i] = new(interface{})
		if i != 0 {
			fmt.Print("\t")
		}
		fmt.Print(cols[i])
	}
	fmt.Println()
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			fmt.Println(err)
			continue
		}
		for i := 0; i < len(vals); i++ {
			if i != 0 {
				fmt.Print("\t")
			}
			printValue(vals[i].(*interface{}))
		}
		fmt.Println()

	}
	if rows.Err() != nil {
		return rows.Err()
	}
	return nil
}

func printValue(pval *interface{}) {
	switch v := (*pval).(type) {
	case nil:
		fmt.Print("NULL")
	case bool:
		if v {
			fmt.Print("1")
		} else {
			fmt.Print("0")
		}
	case []byte:
		fmt.Print(string(v))
	case time.Time:
		fmt.Print(v.Format("2006-01-02 15:04:05.999"))
	default:
		fmt.Print(v)
	}
}
