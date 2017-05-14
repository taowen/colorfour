package take3

import (
	"github.com/taowen/sqlxx"
	"errors"
)

var updateBalanceSql = sqlxx.Translate(
	`UPDATE balance SET amount=amount+:delta
	WHERE account_id=:account_id AND amount+:delta > 0`)
var insertBalanceUpdateEventSql = sqlxx.Translate(
	`INSERT INTO balance_update_event :INSERT_COLUMNS`,
	"balance_update_event_id", "account_id", "delta")
var getBalanceUpdateEventSql = sqlxx.Translate(
	`SELECT * FROM balance_update_event
	WHERE balance_update_event_id=:balance_update_event_id`)

func Transfer(conn *sqlxx.Conn, referenceNumber, from, to string, amount int) (err error) {
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
	err = doTransfer(conn, referenceNumber, from, to, amount)
	return
}


func doTransfer(conn *sqlxx.Conn, referenceNumber, from, to string, amount int) (err error) {
	if err := mayUpdateBalance(conn, referenceNumber, from, -int64(amount)); err != nil {
		return err
	}
	return mayUpdateBalance(conn, referenceNumber, to, int64(amount))
}

func mayUpdateBalance(conn *sqlxx.Conn, referenceNumber, accountId string, delta int64) error {
	shouldUpdateBalance, err := insertBalanceUpdateEvent(conn, referenceNumber, accountId, delta)
	if err != nil {
		return err
	}
	if !shouldUpdateBalance {
		return nil
	}
	return updateBalance(conn, accountId, delta)
}

func insertBalanceUpdateEvent(conn *sqlxx.Conn, referenceNumber, accountId string, delta int64) (bool, error) {
	balanceUpdateEventId := referenceNumber + "_" + accountId
	stmt := conn.Statement(insertBalanceUpdateEventSql)
	defer stmt.Close()
	_, err := stmt.Exec(
		"balance_update_event_id", balanceUpdateEventId,
		"account_id", accountId,
		"delta", delta)
	if err != nil {
		if isBalanceUpdateEventExists(conn, balanceUpdateEventId) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isBalanceUpdateEventExists(conn *sqlxx.Conn, balanceUpdateEventId string) bool {
	stmt := conn.Statement(getBalanceUpdateEventSql)
	defer stmt.Close()
	rows, err := stmt.Query("balance_update_event_id", balanceUpdateEventId)
	if err != nil {
		return false
	}
	defer rows.Close()
	err = rows.Next()
	return err == nil
}

func updateBalance(conn *sqlxx.Conn, accountId string, delta int64) error {
	stmt := conn.Statement(updateBalanceSql)
	defer stmt.Close()
	result, err := stmt.Exec("account_id", accountId, "delta", delta)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if rowsAffected == 0 {
		err = errors.New("not enough balance")
		return err
	}
	return nil
}
