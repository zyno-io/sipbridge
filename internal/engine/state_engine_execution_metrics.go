package engine

import "sipbridge/internal/execution"

func (e *StateEngine) ExecutionStatistics() []execution.Statistic {
	if source, ok := e.runtime.(interface{ Statistics() []execution.Statistic }); ok {
		return source.Statistics()
	}
	return nil
}
