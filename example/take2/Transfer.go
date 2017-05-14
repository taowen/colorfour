package take2

import (
	"github.com/taowen/sqlxx"
	"errors"
)

var updateAmountSql = sqlxx.Translate(
	`UPDATE account SET amount=amount+:delta
	WHERE account_id=:account_id AND amount+:delta > 0`)

func Transfer(conn *sqlxx.Conn, from, to string, amount int) (err error) {
	err = conn.BeginTx()
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			conn.RollbackTx()
		} else {
			conn.CommitTx()
		}
	}()
	err = doTransfer(conn, from, to, amount)
	return
}

func doTransfer(conn *sqlxx.Conn, from, to string, amount int) error {
	stmt := conn.Statement(updateAmountSql)
	defer stmt.Close()
	result, err := stmt.Exec("account_id", from, "delta", int64(-amount))
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if rowsAffected == 0 {
		err = errors.New("not enough balance")
		return err
	}
	_, err = stmt.Exec("account_id", to, "delta", int64(amount))
	return err
}
