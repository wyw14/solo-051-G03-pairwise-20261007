package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/service"
)

func createBatch(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input batch.CreateBatchInput
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			writeBatchError(w, http.StatusBadRequest, "invalid request body")
			return
		}
		detail, err := svc.CreateBatch(r.Context(), input)
		if err != nil {
			writeBatchErr(w, err)
			return
		}
		w.WriteHeader(http.StatusCreated)
		writeJSON(w, detail)
	}
}

func listBatches(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, svc.BatchesList()) }
}

func getBatch(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseBatchID(w, r)
		if !ok {
			return
		}
		detail, err := svc.BatchDetail(id)
		if err != nil {
			writeBatchErr(w, err)
			return
		}
		writeJSON(w, detail)
	}
}

func batchItems(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseBatchID(w, r)
		if !ok {
			return
		}
		items, err := svc.BatchItems(id)
		if err != nil {
			writeBatchErr(w, err)
			return
		}
		writeJSON(w, items)
	}
}

func controlBatch(svc *service.Service, action string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := parseBatchID(w, r)
		if !ok {
			return
		}
		var (
			detail *batch.Detail
			err    error
		)
		switch action {
		case "pause":
			detail, err = svc.PauseBatch(id)
		case "resume":
			detail, err = svc.ResumeBatch(id)
		case "cancel":
			detail, err = svc.CancelBatch(id)
		default:
			writeBatchError(w, http.StatusBadRequest, "unknown batch action")
			return
		}
		if err != nil {
			writeBatchErr(w, err)
			return
		}
		writeJSON(w, detail)
	}
}

func parseBatchID(w http.ResponseWriter, r *http.Request) (model.ID, bool) {
	id, ok := model.ParseID(chi.URLParam(r, "batchID"))
	if !ok {
		writeBatchError(w, http.StatusBadRequest, "invalid batch id")
		return "", false
	}
	return id, true
}

func writeBatchErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, batch.ErrBatchNotFound):
		writeBatchError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, batch.ErrInvalidTransition):
		writeBatchError(w, http.StatusConflict, err.Error())
	default:
		// Validation failures (empty batch, duplicate key, cycle, missing
		// dependency, unknown recipe) are client errors.
		writeBatchError(w, http.StatusBadRequest, err.Error())
	}
}

func writeBatchError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	writeJSON(w, map[string]string{"error": message})
}
