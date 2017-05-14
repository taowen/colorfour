package take1

import (
	"github.com/taowen/sqlxx"
	"errors"
)

var queryAmountSql = sqlxx.Translate("SELECT * FROM take1.account WHERE account_id=:account_id")
var updateAmountSql = sqlxx.Translate(`UPDATE account SET amount=amount+:delta WHERE account_id=:account_id`)

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
	err = assertBalanceIsEnough(conn, from, amount)
	if err != nil {
		return
	}
	stmt := conn.Statement(updateAmountSql)
	defer stmt.Close()
	_, err = stmt.Exec("account_id", from, "delta", int64(-amount))
	if err != nil {
		return
	}
	_, err = stmt.Exec("account_id", to, "delta", int64(amount))
	return
}

func assertBalanceIsEnough(conn *sqlxx.Conn, accountId string, toTransferOutAmount int) error {
	currentAmount, err := queryAmount(conn, accountId)
	if err != nil {
		return err
	}
	if currentAmount < toTransferOutAmount {
		return errors.New("not enough balance")
	}
	return nil
}

func queryAmount(conn *sqlxx.Conn, accountId string) (int, error) {
	stmt := conn.Statement(queryAmountSql)
	defer stmt.Close()
	rows, err := stmt.Query("account_id", accountId)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if err := rows.Next(); err != nil {
		return 0, err
	}
	return rows.GetInt(rows.C("amount")), nil
}