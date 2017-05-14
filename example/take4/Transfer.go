package take4

import (
	"github.com/taowen/sqlxx"
	"github.com/taowen/colorfour/tristate"
)

func Transfer(conn *sqlxx.Conn, referenceNumber, from, to string, amount int) *tristate.TriState {
	result := updateBalance(conn, referenceNumber+"_"+from, from, -int64(amount))
	if result.IsFailure() || result.IsUnknown() {
		return result
	}
	result = updateBalance(conn, referenceNumber+"_"+to, to, int64(amount))
	if result.IsSuccess() || result.IsUnknown() {
		return result
	}
	rollbackResult := updateBalance(conn, referenceNumber+"_"+from+"_rollback", from, int64(amount))
	if rollbackResult.IsSuccess() {
		return result
	}
	return rollbackResult
}
