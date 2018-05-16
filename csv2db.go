package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strings"
	"time"
)

type Column struct {
	Name   string
	Length int
	Order  int
}

type Columns map[string]*Column

func MakeCol(Name string, Length int, Order int) *Column {
	var ret Column
	ret.Name = Name
	ret.Length = Length
	ret.Order = Order
	return &ret
}
func usage() {
	fmt.Printf("usage csv2db [filename]\n")
	flag.PrintDefaults()
	os.Exit(2)
}

type Config struct {
	Inputfile     *string
	Delimeter     *string
	Dbname        *string
	Dbhost        *string
	Dbport        *string
	Dbuser        *string
	Dbpass        *string
	Dbschema      *string
	Dbtablename   *string
	FileColumnRow *int
	FileDataRow   *int
	PgConstr      string
}

var cfg Config

func init() {

	cfg.Inputfile = flag.String("inputfile", "testfile.txt", "Input File CSV")
	cfg.Delimeter = flag.String("delimeter", "|", "Column Delimiter")
	cfg.Dbname = flag.String("dbname", "March_2018", "Database Name")
	cfg.Dbhost = flag.String("dbhost", "localhost", "Database Host")
	cfg.Dbport = flag.String("dbport", "5433", "Database Port")
	cfg.Dbuser = flag.String("dbuser", "postgres", "Database User")
	cfg.Dbpass = flag.String("dbpass", "joatmos", "Database Password")
	cfg.Dbschema = flag.String("dbschema", "public", "Database Schema")
	cfg.Dbtablename = flag.String("dbtablename", "input", "Database Table Name")
	cfg.FileColumnRow = flag.Int("filecolumnrow", 0, "Row of Data with Column Names")
	cfg.FileDataRow = flag.Int("filedatarow", 0, "Row of Data that Data starts on")
}

func CreateTable(cfg Config) {
	fmt.Printf("TableCreate Start Time: %v\n", time.Now())

	var delim = *cfg.Delimeter
	f, err := os.Open(*cfg.Inputfile)
	if err != nil {
		fmt.Println(err)
	}
	scanner := bufio.NewScanner(f)

	m := make(Columns)
	var colNames []string
	var linecount = 0
	var onum = 0
	var fcr = *cfg.FileColumnRow
	var fdr = *cfg.FileDataRow
	for scanner.Scan() {
		if linecount == fcr {
			colLine := scanner.Text()
			colNames = strings.Split(colLine, delim)
			for c := range colNames {
				m[colNames[c]] = MakeCol(colNames[c], 0, onum)
				onum += 1
			}
		} else {
			if linecount >= fdr {
				line := scanner.Text()
				var columns = strings.Split(line, delim)
				if len(columns) == len(colNames) {
					for i := range columns {
						var curLen = m[colNames[i]].Length
						if len(columns[i]) > curLen {
							m[colNames[i]].Length = len(columns[i])
						}
					}
				}
			}
		}
		linecount += 1
	}
	var tableSQL strings.Builder
	tableSQL.WriteString(fmt.Sprintf("create table \"%s\".\"%s\" (\n id serial8 not null,\n", *cfg.Dbschema, *cfg.Dbtablename))
	for i := 0; i < onum; i++ {
		for g, v := range m {
			if i == v.Order {
				if v.Length > 255 {
					tableSQL.WriteString(fmt.Sprintf("\"%s\" text,\n", g))
				} else {
					tableSQL.WriteString(fmt.Sprintf("\"%s\" varchar(255),\n", g))
				}
			}
		}
	}
	tableSQL.WriteString("primary key(id));")
	// lazily open db (doesn't truly open until first request)
	db, err := sql.Open("postgres", cfg.PgConstr)
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Prepare(tableSQL.String())
	if err != nil {
		log.Fatal(err)
	}

	res, err := stmt.Exec()
	if err != nil || res != nil {
		log.Fatal(err)
	}
	fmt.Printf("Table Create Stop Time: %v\n", time.Now())
	db.Close()
	f.Close()
}

func AddDatatoTable(cfg Config) {
	fd, err := os.Open(*cfg.Inputfile)
	var fcr = *cfg.FileColumnRow
	var fdr = *cfg.FileDataRow
	m := make(Columns)
	var colNames []string

	var onum = 0
	var linecount = 0
	if err != nil {
		fmt.Println(err)
	}
	scanner := bufio.NewScanner(fd)
	var dvs []string
	for scanner.Scan() {
		if linecount == fcr {
			colLine := scanner.Text()
			colNames = strings.Split(colLine, *cfg.Delimeter)
			for c := range colNames {
				m[colNames[c]] = MakeCol(colNames[c], 0, onum)
				onum += 1
			}
			dvs = make([]string, len(colNames))
			for i := 0; i < len(colNames); i++ {
				dvs[i] = fmt.Sprintf("$%d", i+1)
			}
		} else {
			if linecount >= fdr {
				line := scanner.Text()
				var columns = strings.Split(line, *cfg.Delimeter)
				var sStmt = fmt.Sprintf("insert into \"%s\".\"%s\" (\"%s\") values('%s') returning id",
					*cfg.Dbschema,
					*cfg.Dbtablename,
					strings.Join(colNames, "\",\""),
					strings.Join(columns, "','"))
				insertLine(cfg, linecount, sStmt, columns)
			}
		}
		linecount += 1
	}

}

func insertLine(cfg Config, lineId int, sStmt string, columns []string) {
	db, err := sql.Open("postgres", cfg.PgConstr)
	if err != nil {
		log.Fatal(err)
	}
	var rowid int
	err = db.QueryRow(sStmt).Scan(rowid)
	if err != nil {
		log.Fatal(err)
	}
	db.Close()
}

func main() {
	flag.Parse()
	fmt.Println("TableName", *cfg.Dbtablename, " Schema:", *cfg.Dbschema)
	cfg.PgConstr = fmt.Sprintf("host=%s dbname=%s user=%s password=%s port=%s sslmode=disable", *cfg.Dbhost, *cfg.Dbname, *cfg.Dbuser, *cfg.Dbpass, *cfg.Dbport)
	CreateTable(cfg)
	AddDatatoTable(cfg)
}
