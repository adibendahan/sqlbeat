package beater

import (
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/adibendahan/sqlbeat/config"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	// sql go drivers
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Sqlbeat is a struct to hold the beat config & info
type Sqlbeat struct {
	beatConfig      *config.Config
	done            chan struct{}
	period          time.Duration
	dbType          string
	hostname        string
	port            string
	username        string
	password        string
	passwordAES     string
	database        string
	postgresSSLMode string
	queries         []string
	queryTypes      []string
	deltaWildcard   string

	oldValues    common.MapStr
	oldValuesAge common.MapStr
}

var (
	commonIV = []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}
)

const (
	// secret length must be 16, 24 or 32, corresponding to the AES-128, AES-192 or AES-256 algorithms
	// you should compile your sqlbeat with a unique secret and hide it (don't leave it in the code after compiled)
	// you can encrypt your password with github.com/adibendahan/sqlbeat-password-encrypter just update your secret
	// (and commonIV if you choose to change it) and compile.
	secret = "github.com/adibendahan/mysqlbeat"

	// supported DB types
	dbtMySQL = "mysql"
	dbtMSSQL = "mssql"
	dbtPSQL  = "postgres"

	// default values
	defaultPeriod        = "10s"
	defaultHostname      = "127.0.0.1"
	defaultPortMySQL     = "3306"
	defaultPortMSSQL     = "1433"
	defaultPortPSQL      = "5432"
	defaultUsername      = "sqlbeat_user"
	defaultPassword      = "sqlbeat_pass"
	defaultDeltaWildcard = "__DELTA"

	// query types values
	queryTypeSingleRow    = "single-row"
	queryTypeMultipleRows = "multiple-rows"
	queryTypeTwoColumns   = "two-columns"
	queryTypeSlaveDelay   = "show-slave-delay"

	// special column names values
	columnNameSlaveDelay = "Seconds_Behind_Master"

	// column types values
	columnTypeString = iota
	columnTypeInt
	columnTypeFloat
)

// New Creates beater
func New() *Sqlbeat {
	return &Sqlbeat{
		done: make(chan struct{}),
	}
}

///*** Beater interface methods ***///

// Config is a function to read config file
func (bt *Sqlbeat) Config(b *beat.Beat) error {

	// Load beater beatConfig
	err := cfgfile.Read(&bt.beatConfig, "")
	if err != nil {
		return fmt.Errorf("Error reading config file: %v", err)
	}

	return nil
}

// Setup is a function to setup all beat config & info into the beat struct
func (bt *Sqlbeat) Setup(b *beat.Beat) error {

	// Config errors handling
	switch bt.beatConfig.Sqlbeat.DBType {
	case dbtMSSQL, dbtMySQL, dbtPSQL:
		break
	default:
		err := fmt.Errorf("Unknown DB type, supported DB types: `mssql`, `mysql`, `postgres`")
		return err
	}

	if len(bt.beatConfig.Sqlbeat.Queries) < 1 {
		err := fmt.Errorf("There are no queries to execute")
		return err
	}

	if len(bt.beatConfig.Sqlbeat.Queries) != len(bt.beatConfig.Sqlbeat.QueryTypes) {
		err := fmt.Errorf("Config file error, queries != queryTypes array length (each query should have a corresponding type on the same index)")
		return err
	}

	if bt.beatConfig.Sqlbeat.DBType == dbtPSQL {
		if bt.beatConfig.Sqlbeat.Database == "" {
			err := fmt.Errorf("Database must be selected when using DB type postgres")
			return err
		}
		if bt.beatConfig.Sqlbeat.PostgresSSLMode == "" {
			err := fmt.Errorf("PostgresSSLMode must be selected when using DB type postgres")
			return err
		}
	}

	// Setting defaults for missing config
	if bt.beatConfig.Sqlbeat.Period == "" {
		logp.Info("Period not selected, proceeding with '%v' as default", defaultPeriod)
		bt.beatConfig.Sqlbeat.Period = defaultPeriod
	}

	if bt.beatConfig.Sqlbeat.Hostname == "" {
		logp.Info("Hostname not selected, proceeding with '%v' as default", defaultHostname)
		bt.beatConfig.Sqlbeat.Hostname = defaultHostname
	}

	if bt.beatConfig.Sqlbeat.Port == "" {
		switch bt.beatConfig.Sqlbeat.DBType {
		case dbtMSSQL:
			bt.beatConfig.Sqlbeat.Port = defaultPortMSSQL
		case dbtMySQL:
			bt.beatConfig.Sqlbeat.Port = defaultPortMySQL
		case dbtPSQL:
			bt.beatConfig.Sqlbeat.Port = defaultPortPSQL
		}
		logp.Info("Port not selected, proceeding with '%v' as default", bt.beatConfig.Sqlbeat.Port)
	}

	if bt.beatConfig.Sqlbeat.Username == "" {
		logp.Info("Username not selected, proceeding with '%v' as default", defaultUsername)
		bt.beatConfig.Sqlbeat.Username = defaultUsername
	}

	if bt.beatConfig.Sqlbeat.Password == "" && bt.beatConfig.Sqlbeat.EncryptedPassword == "" {
		logp.Info("Password not selected, proceeding with default password")
		bt.beatConfig.Sqlbeat.Password = defaultPassword
	}

	if bt.beatConfig.Sqlbeat.DeltaWildcard == "" {
		logp.Info("DeltaWildcard not selected, proceeding with '%v' as default", defaultDeltaWildcard)
		bt.beatConfig.Sqlbeat.DeltaWildcard = defaultDeltaWildcard
	}

	// Parse the Period string
	var durationParseError error
	bt.period, durationParseError = time.ParseDuration(bt.beatConfig.Sqlbeat.Period)
	if durationParseError != nil {
		return durationParseError
	}

	// Handle password decryption and save in the bt
	if bt.beatConfig.Sqlbeat.Password != "" {
		bt.password = bt.beatConfig.Sqlbeat.Password
	} else if bt.beatConfig.Sqlbeat.EncryptedPassword != "" {
		aesCipher, err := aes.NewCipher([]byte(secret))
		if err != nil {
			return err
		}
		cfbDecrypter := cipher.NewCFBDecrypter(aesCipher, commonIV)
		chiperText, err := hex.DecodeString(bt.beatConfig.Sqlbeat.EncryptedPassword)
		if err != nil {
			return err
		}
		plaintextCopy := make([]byte, len(chiperText))
		cfbDecrypter.XORKeyStream(plaintextCopy, chiperText)
		bt.password = string(plaintextCopy)
	}

	// init the oldValues and oldValuesAge array
	bt.oldValues = common.MapStr{"sqlbeat": "init"}
	bt.oldValuesAge = common.MapStr{"sqlbeat": "init"}

	// Save config values to the bt
	bt.dbType = bt.beatConfig.Sqlbeat.DBType
	bt.hostname = bt.beatConfig.Sqlbeat.Hostname
	bt.port = bt.beatConfig.Sqlbeat.Port
	bt.username = bt.beatConfig.Sqlbeat.Username
	bt.database = bt.beatConfig.Sqlbeat.Database
	bt.postgresSSLMode = bt.beatConfig.Sqlbeat.PostgresSSLMode
	bt.queries = bt.beatConfig.Sqlbeat.Queries
	bt.queryTypes = bt.beatConfig.Sqlbeat.QueryTypes
	bt.deltaWildcard = bt.beatConfig.Sqlbeat.DeltaWildcard

	logp.Info("Total # of queries to execute: %d", len(bt.queries))
	for index, queryStr := range bt.queries {
		logp.Info("Query #%d (type: %s): %s", index+1, bt.queryTypes[index], queryStr)
	}

	return nil
}

// Run is a functions that runs the beat
func (bt *Sqlbeat) Run(b *beat.Beat) error {
	logp.Info("sqlbeat is running! Hit CTRL-C to stop it.")

	ticker := time.NewTicker(bt.period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		err := bt.beat(b)
		if err != nil {
			return err
		}
	}
}

// Cleanup is a function that does nothing on this beat :)
func (bt *Sqlbeat) Cleanup(b *beat.Beat) error {
	return nil
}

// Stop is a function that runs once the beat is stopped
func (bt *Sqlbeat) Stop() {
	close(bt.done)
}

///*** sqlbeat methods ***///

// beat is a function that iterate over the query array, generate and publish events
func (bt *Sqlbeat) beat(b *beat.Beat) error {

	connString := ""

	switch bt.dbType {
	case dbtMSSQL:
		connString = fmt.Sprintf("server=%v;user id=%v;password=%v;port=%v;database=%v",
			bt.hostname, bt.username, bt.password, bt.port, bt.database)

	case dbtMySQL:
		connString = fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
			bt.username, bt.password, bt.hostname, bt.port, bt.database)

	case dbtPSQL:
		connString = fmt.Sprintf("%v://%v:%v@%v:%v/%v?sslmode=%v",
			dbtPSQL, bt.username, bt.password, bt.hostname, bt.port, bt.database, bt.postgresSSLMode)
	}

	db, err := sql.Open(bt.dbType, connString)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create a two-columns event for later use
	var twoColumnEvent common.MapStr

LoopQueries:
	for index, queryStr := range bt.queries {
		// Log the query run time and run the query
		dtNow := time.Now()
		rows, err := db.Query(queryStr)
		if err != nil {
			return err
		}

		// Populate columns array
		columns, err := rows.Columns()
		if err != nil {
			return err
		}

		// Populate the two-columns event
		if bt.queryTypes[index] == queryTypeTwoColumns {
			twoColumnEvent = common.MapStr{
				"@timestamp": common.Time(dtNow),
				"type":       bt.dbType,
			}
		}

	LoopRows:
		for rows.Next() {

			switch bt.queryTypes[index] {
			case queryTypeSingleRow, queryTypeSlaveDelay:
				// Generate an event from the current row
				event, err := bt.generateEventFromRow(rows, columns, bt.queryTypes[index], dtNow)

				if err != nil {
					logp.Err("Query #%v error generating event from rows: %v", index, err)
				} else if event != nil {
					b.Events.PublishEvent(event)
					logp.Info("%v event sent", bt.queryTypes[index])
				}
				// breaking after the first row
				break LoopRows

			case queryTypeMultipleRows:
				// Generate an event from the current row
				event, err := bt.generateEventFromRow(rows, columns, bt.queryTypes[index], dtNow)

				if err != nil {
					logp.Err("Query #%v error generating event from rows: %v", index, err)
					break LoopRows
				} else if event != nil {
					b.Events.PublishEvent(event)
					logp.Info("%v event sent", bt.queryTypes[index])
				}

				// Move to the next row
				continue LoopRows

			case queryTypeTwoColumns:
				// append current row to the two-columns event
				err := bt.appendRowToEvent(twoColumnEvent, rows, columns, dtNow)

				if err != nil {
					logp.Err("Query #%v error appending two-columns event: %v", index, err)
					break LoopRows
				}

				// Move to the next row
				continue LoopRows
			}
		}

		// If the two-columns event has data, publish it
		if bt.queryTypes[index] == queryTypeTwoColumns && len(twoColumnEvent) > 2 {
			b.Events.PublishEvent(twoColumnEvent)
			logp.Info("%v event sent", queryTypeTwoColumns)
			twoColumnEvent = nil
		}

		rows.Close()
		if err = rows.Err(); err != nil {
			logp.Err("Query #%v error closing rows: %v", index, err)
			continue LoopQueries
		}
	}

	// Great success!
	return nil
}

// appendRowToEvent appends the two-column event the current row data
func (bt *Sqlbeat) appendRowToEvent(event common.MapStr, row *sql.Rows, columns []string, rowAge time.Time) error {

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// Copy the references into such a []interface{} for row.Scan
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Get RawBytes from data
	err := row.Scan(scanArgs...)
	if err != nil {
		return err
	}

	// First column is the name, second is the value
	strColName := string(values[0])
	strColValue := string(values[1])
	strColType := columnTypeString

	// Try to parse the value to an int64
	nColValue, err := strconv.ParseInt(strColValue, 0, 64)
	if err == nil {
		strColType = columnTypeInt
	}

	// Try to parse the value to a float64
	fColValue, err := strconv.ParseFloat(strColValue, 64)
	if err == nil {
		// If it's not already an established int64, set type to float
		if strColType == columnTypeString {
			strColType = columnTypeFloat
		}
	}

	// If the column name ends with the deltaWildcard
	if strings.HasSuffix(strColName, bt.deltaWildcard) {
		var exists bool
		_, exists = bt.oldValues[strColName]

		// If an older value doesn't exist
		if !exists {
			// Save the current value in the oldValues array
			bt.oldValuesAge[strColName] = rowAge

			if strColType == columnTypeString {
				bt.oldValues[strColName] = strColValue
			} else if strColType == columnTypeInt {
				bt.oldValues[strColName] = nColValue
			} else if strColType == columnTypeFloat {
				bt.oldValues[strColName] = fColValue
			}
		} else {
			// If found the old value's age
			if dtOldAge, ok := bt.oldValuesAge[strColName].(time.Time); ok {
				delta := rowAge.Sub(dtOldAge)

				if strColType == columnTypeInt {
					var calcVal int64

					// Get old value
					oldVal, _ := bt.oldValues[strColName].(int64)
					if nColValue > oldVal {
						// Calculate the delta
						devResult := float64((nColValue - oldVal)) / float64(delta.Seconds())
						// Round the calculated result back to an int64
						calcVal = roundF2I(devResult, .5)
					} else {
						calcVal = 0
					}

					// Add the delta value to the event
					event[strColName] = calcVal

					// Save current values as old values
					bt.oldValues[strColName] = nColValue
					bt.oldValuesAge[strColName] = rowAge
				} else if strColType == columnTypeFloat {
					var calcVal float64

					// Get old value
					oldVal, _ := bt.oldValues[strColName].(float64)
					if fColValue > oldVal {
						// Calculate the delta
						calcVal = (fColValue - oldVal) / float64(delta.Seconds())
					} else {
						calcVal = 0
					}

					// Add the delta value to the event
					event[strColName] = calcVal

					// Save current values as old values
					bt.oldValues[strColName] = fColValue
					bt.oldValuesAge[strColName] = rowAge
				} else {
					event[strColName] = strColValue
				}
			}
		}
	} else { // Not a delta column, add the value to the event as is
		if strColType == columnTypeString {
			event[strColName] = strColValue
		} else if strColType == columnTypeInt {
			event[strColName] = nColValue
		} else if strColType == columnTypeFloat {
			event[strColName] = fColValue
		}
	}

	// Great success!
	return nil
}

// generateEventFromRow creates a new event from the row data and returns it
func (bt *Sqlbeat) generateEventFromRow(row *sql.Rows, columns []string, queryType string, rowAge time.Time) (common.MapStr, error) {

	// Make a slice for the values
	values := make([]sql.RawBytes, len(columns))

	// Copy the references into such a []interface{} for row.Scan
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Create the event and populate it
	event := common.MapStr{
		"@timestamp": common.Time(rowAge),
		"type":       bt.dbType,
	}

	// Get RawBytes from data
	err := row.Scan(scanArgs...)
	if err != nil {
		return nil, err
	}

	// Loop on all columns
	for i, col := range values {
		// Get column name and string value
		strColName := string(columns[i])
		strColValue := string(col)
		strColType := columnTypeString

		// Skip column proccessing when query type is show-slave-delay and the column isn't Seconds_Behind_Master
		if queryType == queryTypeSlaveDelay && strColName != columnNameSlaveDelay {
			continue
		}

		// Try to parse the value to an int64
		nColValue, err := strconv.ParseInt(strColValue, 0, 64)
		if err == nil {
			strColType = columnTypeInt
		}

		// Try to parse the value to a float64
		fColValue, err := strconv.ParseFloat(strColValue, 64)
		if err == nil {
			// If it's not already an established int64, set type to float
			if strColType == columnTypeString {
				strColType = columnTypeFloat
			}
		}

		// If query type is single row and the column name ends with the deltaWildcard
		if queryType == queryTypeSingleRow && strings.HasSuffix(strColName, bt.deltaWildcard) {
			var exists bool
			_, exists = bt.oldValues[strColName]

			// If an older value doesn't exist
			if !exists {
				// Save the current value in the oldValues array
				bt.oldValuesAge[strColName] = rowAge

				if strColType == columnTypeString {
					bt.oldValues[strColName] = strColValue
				} else if strColType == columnTypeInt {
					bt.oldValues[strColName] = nColValue
				} else if strColType == columnTypeFloat {
					bt.oldValues[strColName] = fColValue
				}
			} else {
				// If found the old value's age
				if dtOldAge, ok := bt.oldValuesAge[strColName].(time.Time); ok {
					delta := rowAge.Sub(dtOldAge)

					if strColType == columnTypeInt {
						var calcVal int64

						// Get old value
						oldVal, _ := bt.oldValues[strColName].(int64)

						if nColValue > oldVal {
							// Calculate the delta
							devResult := float64((nColValue - oldVal)) / float64(delta.Seconds())
							// Round the calculated result back to an int64
							calcVal = roundF2I(devResult, .5)
						} else {
							calcVal = 0
						}

						// Add the delta value to the event
						event[strColName] = calcVal

						// Save current values as old values
						bt.oldValues[strColName] = nColValue
						bt.oldValuesAge[strColName] = rowAge
					} else if strColType == columnTypeFloat {
						var calcVal float64
						oldVal, _ := bt.oldValues[strColName].(float64)

						if fColValue > oldVal {
							// Calculate the delta
							calcVal = (fColValue - oldVal) / float64(delta.Seconds())
						} else {
							calcVal = 0
						}

						// Add the delta value to the event
						event[strColName] = calcVal

						// Save current values as old values
						bt.oldValues[strColName] = fColValue
						bt.oldValuesAge[strColName] = rowAge
					} else {
						event[strColName] = strColValue
					}
				}
			}
		} else { // Not a delta column, add the value to the event as is
			if strColType == columnTypeString {
				event[strColName] = strColValue
			} else if strColType == columnTypeInt {
				event[strColName] = nColValue
			} else if strColType == columnTypeFloat {
				event[strColName] = fColValue
			}
		}
	}

	// If the event has no data, set to nil
	if len(event) == 2 {
		event = nil
	}

	return event, nil
}

// roundF2I is a function that returns a rounded int64 from a float64
func roundF2I(val float64, roundOn float64) (newVal int64) {
	var round float64

	digit := val
	_, div := math.Modf(digit)
	if div >= roundOn {
		round = math.Ceil(digit)
	} else {
		round = math.Floor(digit)
	}

	return int64(round)
}
