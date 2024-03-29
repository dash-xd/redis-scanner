package redisscanner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/dash-xd/redis-scan-callbacks"
	"github.com/redis/go-redis/v9"
)

type ScanHandlerOptions struct {
	Env          string
	ParentNS     string
	ChildNS      string
	Entity       string
	Pattern      string
	RedisClient  *redis.Client
	CallbackKeys []string
}

func BuildScanHandler(options ScanHandlerOptions) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        envParam := r.URL.Query().Get(options.Env)
        parentNamespaceParam := r.URL.Query().Get(options.ParentNS)
        childNamespaceParam := r.URL.Query().Get(options.ChildNS)
        startingCursorParam := r.URL.Query().Get("cursor")

        pattern := fmt.Sprintf(options.Pattern, options.Entity, envParam, parentNamespaceParam, childNamespaceParam)

        // Log relevant data
        fmt.Printf("Pattern: %s\n", pattern)
        fmt.Printf("Starting cursor: %s\n", startingCursorParam)
        fmt.Printf("Callback keys: %v\n", options.CallbackKeys)

        keys, nextCursor, err := RunScanWithCallbacks(pattern, parseCursor(startingCursorParam), options.RedisClient, options.CallbackKeys...)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }

        jsonResponse := struct {
            Keys   []string `json:"keys"`
            Cursor uint64   `json:"cursor"`
        }{
            Keys:   keys,
            Cursor: nextCursor,
        }
        json.NewEncoder(w).Encode(jsonResponse)
    }
}

func RunScanWithCallbacks(pattern string, startingCursor uint64, redisClient *redis.Client, callbackKeys ...string) ([]string, uint64, error) {
    ctx := context.Background()
    keys := make([]string, 0)
    cursor := startingCursor

    cbx := callbacks.NewCallbacks()

    for {
        var err error
        keys, cursor, err = redisClient.Scan(ctx, cursor, pattern, 10).Result()
        if err != nil {
            return nil, 0, fmt.Errorf("error during scan: %v", err)
        }

        for _, key := range keys {
            for _, callbackKey := range callbackKeys {
                callbackFunc, ok := cbx.GetCallbackFunc(callbackKey)
                if ok {
                    _, err := callbackFunc(redisClient, key)
                    if err != nil {
                        fmt.Printf("error executing callback for key %s: %v\n", key, err)
                    }
                }
            }
        }

        if cursor == 0 {
            break
        }
    }
    return keys, cursor, nil
}

func parseCursor(cursorStr string) uint64 {
	cursor, _ := strconv.ParseUint(cursorStr, 10, 64)
	return cursor
}
