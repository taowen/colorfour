package take5

import (
	"testing"
	"github.com/taowen/sqlxx"
	"github.com/stretchr/testify/require"
	"database/sql/driver"
	"github.com/go-sql-driver/mysql"
)

type subTest func(should *require.Assertions, conn *sqlxx.Conn)

var subTests map[string]subTest = map[string]subTest{
	"transfer success": func(should *require.Assertions, conn *sqlxx.Conn) {
		insert(should, conn, "balance",
			"account_id", "acc1",
			"amount", int64(101))
		insert(should, conn, "balance",
			"account_id", "acc1_staging",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2_staging",
			"amount", int64(0))
		should.Nil(Transfer(conn, "tran001", "acc1", "acc2", 100))
		should.Equal(1, queryAmount(should, conn, "acc1"))
		should.Equal(100, queryAmount(should, conn, "acc2"))
	},
	"transfer failed due to destination disabled": func(should *require.Assertions, conn *sqlxx.Conn) {
		insert(should, conn, "balance",
			"account_id", "acc1",
			"amount", int64(101))
		insert(should, conn, "balance",
			"account_id", "acc1_staging",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2",
			"amount", int64(0),
			"disabled", int64(1))
		insert(should, conn, "balance",
			"account_id", "acc2_staging",
			"amount", int64(0))
		result := Transfer(conn, "tran001", "acc1", "acc2", 100)
		should.NotNil(result)
		should.True(result.IsFailure())
		should.Equal(101, queryAmount(should, conn, "acc1"))
		should.Equal(0, queryAmount(should, conn, "acc2"))
	},
	"transfer twice": func(should *require.Assertions, conn *sqlxx.Conn) {
		insert(should, conn, "balance",
			"account_id", "acc1",
			"amount", int64(101))
		insert(should, conn, "balance",
			"account_id", "acc1_staging",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2_staging",
			"amount", int64(0))
		should.Nil(Transfer(conn, "tran001", "acc1", "acc2", 100))
		should.Nil(Transfer(conn, "tran001", "acc1", "acc2", 100))
		should.Equal(1, queryAmount(should, conn, "acc1"))
		should.Equal(100, queryAmount(should, conn, "acc2"))
	},
	"not enough balance to transfer out": func(should *require.Assertions, conn *sqlxx.Conn) {
		insert(should, conn, "balance",
			"account_id", "acc1",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc1_staging",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2",
			"amount", int64(0))
		insert(should, conn, "balance",
			"account_id", "acc2_staging",
			"amount", int64(0))
		should.NotNil(Transfer(conn, "tran002", "acc1", "acc2", 100))
	},
}

func Test_transfer(t *testing.T) {
	for subTestName, subTest := range subTests {
		t.Run(subTestName, func(t *testing.T) {
			should := require.New(t)
			drv := mysql.MySQLDriver{}
			conn, err := sqlxx.Open(drv, "root:123456@tcp(127.0.0.1:3306)/take5")
			should.Nil(err)
			defer conn.Close()
			execute(should, conn, `
			CREATE TABLE IF NOT EXISTS balance(
			account_id VARCHAR(128),
			disabled SMALLINT DEFAULT 0,
			amount INT,
			PRIMARY KEY (account_id)
			)`)
			execute(should, conn, `TRUNCATE TABLE balance`)
			execute(should, conn, `
			CREATE TABLE IF NOT EXISTS balance_update_event(
			balance_update_event_id VARCHAR(128),
			account_id VARCHAR(128),
			delta INT,
			PRIMARY KEY (balance_update_event_id)
			)`)
			execute(should, conn, `TRUNCATE TABLE balance_update_event`)
			subTest(should, conn)
		})
	}
}

func insert(should *require.Assertions, conn *sqlxx.Conn, tableName string, inputs ... driver.Value) {
	columnNames := make([]interface{}, 0, len(inputs)/2)
	for i := 0; i < len(inputs); i += 2 {
		columnNames = append(columnNames, inputs[i])
	}
	stmt := conn.TranslateStatement(
		`INSERT INTO :STR_TABLE :INSERT_COLUMNS`, columnNames...)
	defer stmt.Close()
	inputs = append(inputs, "STR_TABLE")
	inputs = append(inputs, tableName)
	_, err := stmt.Exec(inputs...)
	should.Nil(err)
}

func execute(should *require.Assertions, conn *sqlxx.Conn, sql string) {
	stmt := conn.TranslateStatement(sql)
	defer stmt.Close()
	_, err := stmt.Exec()
	should.Nil(err)
}

func queryAmount(should *require.Assertions, conn *sqlxx.Conn, accountId string) int {
	stmt := conn.TranslateStatement(`SELECT * FROM balance WHERE account_id=:account_id`)
	defer stmt.Close()
	rows, err := stmt.Query("account_id", accountId)
	should.Nil(err)
	defer rows.Close()
	should.Nil(rows.Next())
	return rows.GetInt(rows.C("amount"))
}
