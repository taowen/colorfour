package take5

import (
	"github.com/taowen/sqlxx"
	"github.com/taowen/colorfour/tristate"
	"fmt"
)

type Action func() *tristate.TriState
type Step struct {
	Description string
	Apply       Action
	Rollback    Action
}

func executeStepsEventually(steps []Step) *tristate.TriState {
	executedSteps := []Step{}
	var result *tristate.TriState
	for _, step := range steps {
		fmt.Println(fmt.Sprintf("apply %s", step.Description))
		result = step.Apply()
		if result.IsUnknown() {
			fmt.Println("unknown failure: quit")
			return result
		}
		if result.IsFailure() {
			fmt.Println("faield: start to rollback")
			break
		}
		executedSteps = append(executedSteps, step)
	}
	if result.IsSuccess() {
		fmt.Println("all success")
		return result
	}
	for i := len(executedSteps) - 1; i >= 0; i-- {
		fmt.Println(fmt.Sprintf("rollback %s", executedSteps[i].Description))
		rollbackResult := executedSteps[i].Rollback()
		if rollbackResult.IsFailure() || rollbackResult.IsUnknown() {
			fmt.Println("failed to rollback: quit")
			return rollbackResult
		}
	}
	fmt.Println("all rolled back")
	return result
}

func Transfer(conn *sqlxx.Conn, referenceNumber, from, to string, amount int) *tristate.TriState {
	return executeStepsEventually([]Step{
		{fmt.Sprintf("transfer %v from %v to %v", amount, from, from+"_staging"),
			func() *tristate.TriState {
				return directTransfer(conn, referenceNumber, from, from+"_staging", amount)
			}, func() *tristate.TriState {
			return directTransfer(conn, referenceNumber+"_rollback", from+"_staging", from, amount)
		}},
		{fmt.Sprintf("transfer %v from %v to %v", amount, from+"_staging", to+"_staging"),
			func() *tristate.TriState {
				return directTransfer(conn, referenceNumber, from+"_staging", to+"_staging", amount)
			}, func() *tristate.TriState {
			return directTransfer(conn, referenceNumber+"_rollback", to+"_staging", from+"_staging", amount)
		}},
		{fmt.Sprintf("transfer %v from %v to %v", amount, to+"_staging", to),
			func() *tristate.TriState {
				return directTransfer(conn, referenceNumber, to+"_staging", to, amount)
			}, func() *tristate.TriState {
			return directTransfer(conn, referenceNumber+"_rollback", to, to+"_staging", amount)
		}},
	})
}

func directTransfer(conn *sqlxx.Conn, referenceNumber, from, to string, amount int) *tristate.TriState {
	return executeStepsEventually([]Step{
		{fmt.Sprintf("subtract %v from %v", amount, from),
			func() *tristate.TriState {
				return updateBalance(conn, referenceNumber+"_"+from, from, -int64(amount))
			}, func() *tristate.TriState {
			return updateBalance(conn, referenceNumber+"_"+from+"_rollback", from, int64(amount))
		}},
		{fmt.Sprintf("add %v to %v", amount, to),
			func() *tristate.TriState {
				return updateBalance(conn, referenceNumber+"_"+to, to, int64(amount))
			}, func() *tristate.TriState {
			return updateBalance(conn, referenceNumber+"_"+to+"_rollback", to, -int64(amount))
		}},
	})
}
