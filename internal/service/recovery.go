package service

import (
	"context"
	"fmt"
	"github.com/wyw14/cry-161/internal/chamber"
	"github.com/wyw14/cry-161/internal/journal"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/run"
	"time"
)

type RecoveryAction struct {
	RunID  model.ID  `json:"run_id"`
	Name   string    `json:"name"`
	Status string    `json:"status"`
	Detail string    `json:"detail"`
	At     time.Time `json:"at"`
}

type RecoveryReport struct {
	RunID     model.ID         `json:"run_id"`
	Actions   []RecoveryAction `json:"actions"`
	Recovered bool             `json:"recovered"`
	At        time.Time        `json:"at"`
}

func appendRecovery(report RecoveryReport, name string, err error) RecoveryReport {
	action := RecoveryAction{RunID: report.RunID, Name: name, Status: "completed", At: time.Now().UTC()}
	if err != nil {
		action.Status = "failed"
		action.Detail = err.Error()
	}
	report.Actions = append(report.Actions, action)
	return report
}

func (s *Service) Recover(ctx context.Context, runtime *Runtime, runID model.ID) RecoveryReport {
	report := RecoveryReport{RunID: runID, Actions: make([]RecoveryAction, 0), At: time.Now().UTC()}
	value, ok := s.Runs.Get(runID)
	if !ok {
		report = appendRecovery(report, "load_run", fmt.Errorf("run not found"))
		return report
	}
	report = appendRecovery(report, "load_run", nil)
	shutterErr := runtime.Shutter.Close(ctx)
	report = appendRecovery(report, "close_shutter", shutterErr)
	s.Source.Stop()
	report = appendRecovery(report, "stop_source", nil)
	cooldown := chamber.DefaultCooldownPolicy()
	s.Chamber.SetSourcePower(0)
	cooldownErr := cooldown.Wait(ctx, s.Chamber)
	report = appendRecovery(report, "cool_chamber", cooldownErr)
	incident := runtime.Incidents.Raise(runID, "warning", "run recovered to safe state")
	journalErr := s.Journal.Append(journal.IncidentRaised(incident))
	report = appendRecovery(report, "record_recovery", journalErr)
	value.State = model.Cooling
	s.Runs.Add(value)
	report = appendRecovery(report, "set_cooling", nil)
	report.Recovered = shutterErr == nil && cooldownErr == nil && journalErr == nil
	runtime.Timeline.Add(runID, "recovery", fmt.Sprintf("recovered=%t", report.Recovered))
	return report
}

func (s *Service) Release(runID model.ID, runtime *Runtime, operator string) (model.ReleaseRecord, error) {
	value, ok := s.Runs.Get(runID)
	if !ok {
		return model.ReleaseRecord{}, fmt.Errorf("run not found")
	}
	trace := runtime.Traces.Trace(runID)
	evidence := make([]model.LayerEvidence, 0, len(trace.Thickness))
	for index, sample := range trace.Thickness {
		evidence = append(evidence, model.BuildLayerEvidence(sample, index+1))
	}
	record := model.EvaluateRelease(*value, evidence, runtime.Incidents.ForRun(runID), operator)
	if err := record.Validate(); err != nil {
		return record, err
	}
	if record.Decision != model.ReleaseApproved {
		return record, fmt.Errorf("release rejected")
	}
	if value.State == model.Cooling {
		if err := run.Advance(s.Runs, runID, model.Venting); err != nil {
			return record, err
		}
	}
	return record, nil
}

// AutoRelease evaluates a batch-driven run that has reached venting using the
// same gates as an operator release, attributing the decision to the
// orchestrator. It returns the record and an error when release is rejected.
func (s *Service) AutoRelease(runID model.ID) (model.ReleaseRecord, error) {
	value, ok := s.Runs.Get(runID)
	if !ok {
		return model.ReleaseRecord{}, fmt.Errorf("run not found")
	}
	trace := s.Runtime.Traces.Trace(runID)
	evidence := make([]model.LayerEvidence, 0, len(trace.Thickness))
	for index, sample := range trace.Thickness {
		evidence = append(evidence, model.BuildLayerEvidence(sample, index+1))
	}
	record := model.EvaluateRelease(*value, evidence, s.Runtime.Incidents.ForRun(runID), "batch-orchestrator")
	if err := record.Validate(); err != nil {
		return record, err
	}
	if record.Decision != model.ReleaseApproved {
		return record, fmt.Errorf("automatic release rejected")
	}
	return record, nil
}

func (s *Service) Schedule(ctx context.Context, runtime *Runtime, runID model.ID, layers []model.Layer) ([]run.ScheduledLayer, error) {
	scheduler := run.NewScheduler(1)
	for index, layer := range layers {
		if err := scheduler.Enqueue(runID, layer, len(layers)-index); err != nil {
			return nil, err
		}
	}
	err := scheduler.Execute(ctx, func(callCtx context.Context, id model.ID, layer model.Layer) error {
		return s.RunLayer(callCtx, runtime, id, layer)
	})
	return scheduler.Results(), err
}
