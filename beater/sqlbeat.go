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

	"github.com/elastic/beats/libbeat/beat"
	// "github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/adibendahan/sqlbeat/config"

	// sql go drivers
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Sqlbeat is a struct to hold the beat config & info
type Sqlbeat struct {
	done            chan struct{}
	config      		config.Config
	client 					publisher.Client

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

	defaultPortMySQL     = "3306"
	defaultPortMSSQL     = "1433"
	defaultPortPSQL      = "5432"

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
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	logp.Info(">>> New()")

	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}
	
	
	logp.Info("  Config = \n%+v\n", config)
	bt := &Sqlbeat{
		done: make(chan struct{}),
		config: config,
	}

	if err := bt.Setup(b); err != nil {
		return nil, fmt.Errorf("Error validating config file: %v", err)		
	}

	return bt, nil
}

///*** Beater interface methods ***///

// Config is a function to read config file
// func (bt *Sqlbeat) Config(b *beat.Beat) error {

// 	// Load beater beatConfig
// 	err := cfgfile.Read(&bt.config, "")
// 	if err != nil {
// 		return fmt.Errorf("Error reading config file: %v", err)
// 	}

// 	return nil
// }

// Setup is a function to validate
func (bt *Sqlbeat) Setup(b *beat.Beat) error {
	logp.Info(">>> Setup()")
	// Config errors handling
	switch bt.config.DBType {
	case dbtMSSQL, dbtMySQL, dbtPSQL:
		break
	default:
		err := fmt.Errorf("Unknown DB type, supported DB types: `mssql`, `mysql`, `postgres`")
		return err
	}

	if len(bt.config.Queries) < 1 {
		err := fmt.Errorf("There are no queries to execute")
		return err
	}

	if len(bt.config.Queries) != len(bt.config.QueryTypes) {
		err := fmt.Errorf("Config file error, queries != queryTypes array length (each query should have a corresponding type on the same index)")
		return err
	}

	if bt.config.DBType == dbtPSQL {
		if bt.config.Database == "" {
			err := fmt.Errorf("Database must be selected when using DB type postgres")
			return err
		}
		if bt.config.PostgresSSLMode == "" {
			err := fmt.Errorf("PostgresSSLMode must be selected when using DB type postgres")
			return err
		}
	}

	if bt.config.Port == "" {
		switch bt.config.DBType {
		case dbtMSSQL:
			bt.config.Port = defaultPortMSSQL
		case dbtMySQL:
			bt.config.Port = defaultPortMySQL
		case dbtPSQL:
			bt.config.Port = defaultPortPSQL
		}
		logp.Info("Port not selected, proceeding with '%v' as default", bt.config.Port)
	}


	// Handle password decryption and save in the bt
	// if bt.config.Password != "" {
	// 	bt.password = bt.config.Password
	// } else 
	 if bt.config.EncryptedPassword != "" {
		aesCipher, err := aes.NewCipher([]byte(secret))
		if err != nil {
			return err
		}
		cfbDecrypter := cipher.NewCFBDecrypter(aesCipher, commonIV)
		chiperText, err := hex.DecodeString(bt.config.EncryptedPassword)
		if err != nil {
			return err
		}
		plaintextCopy := make([]byte, len(chiperText))
		cfbDecrypter.XORKeyStream(plaintextCopy, chiperText)
		bt.config.Password = string(plaintextCopy)
	}

	// init the oldValues and oldValuesAge array
	bt.oldValues = common.MapStr{"sqlbeat": "init"}
	bt.oldValuesAge = common.MapStr{"sqlbeat": "init"}

	logp.Info("Total # of queries to execute: %d", len(bt.config.Queries))
	for index, queryStr := range bt.config.Queries {
		logp.Info("Query #%d (type: %s): %s", index+1, bt.config.QueryTypes[index], queryStr)
	}

	return nil
}

// Run is a function that runs the beat
func (bt *Sqlbeat) Run(b *beat.Beat) error {
	logp.Info("sqlbeat is running! Hit CTRL-C to stop it.")

	bt.client = b.Publisher.Connect()
	logp.Info("Connected; ticker period is %v", bt.config.Period)
	ticker := time.NewTicker(bt.config.Period)
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


// Stop is a function that runs once the beat is stopped
func (bt *Sqlbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

///*** sqlbeat methods ***///

// beat is a function that iterate over the query array, generate and publish events
func (bt *Sqlbeat) beat(b *beat.Beat) error {

	connString := ""

	switch bt.config.DBType {
	case dbtMSSQL:
		connString = fmt.Sprintf("server=%v;user id=%v;password=%v;port=%v;database=%v",
			bt.config.Hostname, bt.config.Username, bt.config.Password, bt.config.Port, bt.config.Database)

	case dbtMySQL:
		connString = fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
			bt.config.Username, bt.config.Password, bt.config.Hostname, bt.config.Port, bt.config.Database)

	case dbtPSQL:
		connString = fmt.Sprintf("%v://%v:%v@%v:%v/%v?sslmode=%v",
			dbtPSQL, bt.config.Username, bt.config.Password, bt.config.Hostname, bt.config.Port, bt.config.Database, bt.config.PostgresSSLMode)
	}

	db, err := sql.Open(bt.config.DBType, connString)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create a two-columns event for later use
	var twoColumnEvent common.MapStr

LoopQueries:
	for index, queryStr := range bt.config.Queries {
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
		if bt.config.QueryTypes[index] == queryTypeTwoColumns {
			twoColumnEvent = common.MapStr{
				"@timestamp": common.Time(dtNow),
				"type":       bt.config.DBType,
			}
		}

	LoopRows:
		for rows.Next() {

			switch bt.config.QueryTypes[index] {
			case queryTypeSingleRow, queryTypeSlaveDelay:
				// Generate an event from the current row
				event, err := bt.generateEventFromRow(rows, columns, bt.config.QueryTypes[index], dtNow)

				if err != nil {
					logp.Err("Query #%v error generating event from rows: %v", index, err)
				} else if event != nil {
					// b.Events.PublishEvent(event)
					bt.client.PublishEvent(event)
					logp.Info("%v event sent", bt.config.QueryTypes[index])
				}
				// breaking after the first row
				break LoopRows

			case queryTypeMultipleRows:
				// Generate an event from the current row
				event, err := bt.generateEventFromRow(rows, columns, bt.config.QueryTypes[index], dtNow)

				if err != nil {
					logp.Err("Query #%v error generating event from rows: %v", index, err)
					break LoopRows
				} else if event != nil {
					// b.Events.PublishEvent(event)
					bt.client.PublishEvent(event)
					logp.Info("%v event sent", bt.config.QueryTypes[index])
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
		if bt.config.QueryTypes[index] == queryTypeTwoColumns && len(twoColumnEvent) > 2 {
			bt.client.PublishEvent(twoColumnEvent)
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
	if strings.HasSuffix(strColName, bt.config.DeltaWildcard) {
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
		"type":       bt.config.DBType,
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
		if queryType == queryTypeSingleRow && strings.HasSuffix(strColName, bt.config.DeltaWildcard) {
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
