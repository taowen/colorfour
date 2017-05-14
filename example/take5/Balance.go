package take5

import (
	"errors"
	"github.com/taowen/sqlxx"
	"github.com/taowen/colorfour/tristate"
)

var updateBalanceSql = sqlxx.Translate(
	`UPDATE balance SET amount=amount+:delta
	WHERE account_id=:account_id AND amount+:delta > 0 AND disabled=0`)
var insertBalanceUpdateEventSql = sqlxx.Translate(
	`INSERT INTO balance_update_event :INSERT_COLUMNS`,
	"balance_update_event_id", "account_id", "delta")
var getBalanceUpdateEventSql = sqlxx.Translate(
	`SELECT * FROM balance_update_event
	WHERE balance_update_event_id=:balance_update_event_id`)

func updateBalance(conn *sqlxx.Conn, balanceUpdateEventId, accountId string, delta int64) *tristate.TriState {
	err := conn.BeginTx()
	if err != nil {
		return tristate.NewFailure(err)
	}
	shouldUpdateBalance, err := insertBalanceUpdateEvent(conn, balanceUpdateEventId, accountId, delta)
	if err != nil {
		if err2 := conn.RollbackTx(); err2 != nil {
			return tristate.NewUnknown(err2)
		}
		return tristate.NewFailure(err)
	}
	if !shouldUpdateBalance {
		if err2 := conn.CommitTx(); err2 != nil {
			return tristate.NewUnknown(err2)
		}
		return tristate.NewSuccess()
	}
	result := doUpdateBalance(conn, accountId, delta)
	if result.IsUnknown() {
		return result
	}
	if result.IsFailure() {
		if err2 := conn.RollbackTx(); err2 != nil {
			return tristate.NewUnknown(err2)
		}
		return result
	}
	if err := conn.CommitTx(); err != nil {
		return tristate.NewUnknown(err)
	}
	return tristate.NewSuccess()
}

func insertBalanceUpdateEvent(conn *sqlxx.Conn, balanceUpdateEventId, accountId string, delta int64) (bool, error) {
	stmt := conn.Statement(insertBalanceUpdateEventSql)
	defer stmt.Close()
	_, err := stmt.Exec(
		"balance_update_event_id", balanceUpdateEventId,
		"delta", delta,
		"account_id", accountId)
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

func doUpdateBalance(conn *sqlxx.Conn, accountId string, delta int64) *tristate.TriState {
	stmt := conn.Statement(updateBalanceSql)
	defer stmt.Close()
	result, err := stmt.Exec("account_id", accountId, "delta", delta)
	if err != nil {
		return tristate.NewUnknown(err)
	}
	rowsAffected, err := result.RowsAffected()
	if rowsAffected == 0 {
		err = errors.New("not enough balance or account disabled")
		return tristate.NewFailure(err)
	}
	return tristate.NewSuccess()
}
