package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

type HttpServer struct {
	Log *Log
}

func New(addr string) *http.Server {
	srv := NewHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", srv.handleProduce).Methods(http.MethodPost)
	r.HandleFunc("/", srv.handleConsume).Methods(http.MethodGet)
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

func NewHTTPServer() *HttpServer {
	return &HttpServer{
		Log: NewLog(),
	}
}

type ProduceRequest struct {
	Record Record `json:"record"`
}

type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumeResponse struct {
	Record Record `json:"record"`
}

func (s *HttpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ProduceResponse{Offset: offset}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()
}

func (s *HttpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp := ConsumeResponse{Record: record}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_ = r.Body.Close()
}
