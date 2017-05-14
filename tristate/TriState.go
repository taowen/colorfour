package tristate

type StateValue int

const (
	TRISTATE_SUCCESS StateValue = iota
	TRISTATE_FAILRUE
	TRISTATE_UNKNOWN
)

type TriState struct {
	State StateValue
	Err error
}

func NewSuccess() *TriState {
	return nil
}

func NewUnknown(err error) *TriState {
	if err == nil {
		panic("error not set")
	}
	return &TriState{TRISTATE_UNKNOWN, err}
}

func NewFailure(err error) *TriState {
	if err == nil {
		panic("error not set")
	}
	return &TriState{TRISTATE_FAILRUE, err}
}

func (state *TriState) Error() string {
	return state.Err.Error()
}

func (state *TriState) IsSuccess() bool {
	return state == nil || state.State == TRISTATE_SUCCESS
}

func (state *TriState) IsFailure() bool {
	return state != nil && state.State == TRISTATE_FAILRUE
}

func (state *TriState) IsUnknown() bool {
	return state != nil && state.State == TRISTATE_UNKNOWN
}
