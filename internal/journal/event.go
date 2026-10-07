package journal

import "github.com/wyw14/cry-161/internal/model"

func RunStarted(run model.Run) model.Event {
	return model.NewEvent("run.started", run.ID, map[string]any{"batch": run.Batch, "state": run.State})
}
func RunStateChanged(run model.Run, previous model.RunState) model.Event {
	return model.NewEvent("run.state", run.ID, map[string]any{"from": previous, "to": run.State})
}
func IncidentRaised(incident model.Incident) model.Event {
	return model.NewEvent("incident", incident.RunID, map[string]any{"severity": incident.Severity, "message": incident.Message})
}

func BatchCreated(batchID model.ID, name string, runCount int) model.Event {
	return model.NewEvent("batch.created", "", map[string]any{"batch_id": string(batchID), "name": name, "run_count": runCount})
}
func BatchStateChanged(batchID model.ID, from, to string) model.Event {
	return model.NewEvent("batch.state", "", map[string]any{"batch_id": string(batchID), "from": from, "to": to})
}
func ItemBlocked(batchID, itemID model.ID, reason string) model.Event {
	return model.NewEvent("batch.item_blocked", "", map[string]any{"batch_id": string(batchID), "item_id": string(itemID), "reason": reason})
}
func RunRecovered(runID model.ID) model.Event {
	return model.NewEvent("run.recovered", runID, map[string]any{})
}
