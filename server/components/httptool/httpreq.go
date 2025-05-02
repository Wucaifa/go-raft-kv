package httptool

import (
	"encoding/json"
	"io"
	"net/http"
)

func StringBody(r *http.Request) (string, error) {
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	return string(bytes), err
}

func JsonBody[T any](r *http.Request) (T, error) {
	var t T
	bytes, err := io.ReadAll(r.Body)
	if err != nil {
		return t, err
	}
	json.Unmarshal(bytes, &t)
	return t, err
}
