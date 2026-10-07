package api

import (
	"encoding/json"
	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-161/internal/batch"
	"github.com/wyw14/cry-161/internal/model"
	"github.com/wyw14/cry-161/internal/service"
	"net/http"
)

func listBatches(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) { writeJSON(w, svc.Batches.List()) }
}

func createBatch(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Name      string   `json:"name"`
			Priority  int      `json:"priority"`
			DependsOn []string `json:"depends_on"`
			Runs      []struct {
				RecipeID string `json:"recipe_id"`
			} `json:"runs"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		draft := &batch.Batch{Name: input.Name, Priority: input.Priority}
		for _, value := range input.DependsOn {
			id, ok := model.ParseID(value)
			if !ok {
				http.Error(w, "invalid dependency", 400)
				return
			}
			draft.DependsOn = append(draft.DependsOn, id)
		}
		for _, spec := range input.Runs {
			id, ok := model.ParseID(spec.RecipeID)
			if !ok {
				http.Error(w, "invalid recipe", 400)
				return
			}
			draft.Specs = append(draft.Specs, batch.RunSpec{RecipeID: id})
		}
		created, err := svc.Batches.Submit(draft)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		writeJSON(w, created)
	}
}

func getBatch(svc *service.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := model.ParseID(chi.URLParam(r, "batchID"))
		if !ok {
			http.Error(w, "invalid batch", 400)
			return
		}
		value, found := svc.Batches.Get(id)
		if !found {
			http.Error(w, "batch not found", 404)
			return
		}
		writeJSON(w, value)
	}
}

func batchAction(svc *service.Service, action func(model.ID) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := model.ParseID(chi.URLParam(r, "batchID"))
		if !ok {
			http.Error(w, "invalid batch", 400)
			return
		}
		if err := action(id); err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		value, _ := svc.Batches.Get(id)
		writeJSON(w, value)
	}
}
