package histogram

import (
	"net/http"
	"time"
)

const (
	// dateFormat is Gen, 2, 2006 in the expected format.
	dateFormat = "2006-01-02"
)

// GenerateHandler is a handler for the /v0/generate endpoint.
type GenerateHandler struct {
}

func (h *GenerateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	params := r.URL.Query()
	start := params.Get("start")
	end := params.Get("end")
	if start == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing parameter: from"))
	}
	if end == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Missing parameter: to"))
	}
	// Attempt to parse provided dates to check they are valid and in the right
	// format (as far as we can tell.)
	// The results are discarded since dates are provided to the Generator as
	// strings.
	_, err := time.Parse(dateFormat, start)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid date format (from): " + err.Error()))
	}
	_, err = time.Parse(dateFormat, end)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid date format (to): " + err.Error()))
	}
}
