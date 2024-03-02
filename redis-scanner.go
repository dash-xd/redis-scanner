// redis-scanner package

package redis-scanner

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/dash-xd/redis-scan-callbacks"
	"github.com/redis/go-redis/v9"
)

func RunScan(pattern string, startingCursor uint64, redisClient *redis.Client, callbackKeys ...string) ([]string, uint64, error) {
	ctx := context.Background()
	keys := make([]string, 0)
	cursor := startingCursor

	for {
		var err error
		keys, cursor, err = redisClient.Scan(ctx, cursor, pattern, 10).Result()
		if err != nil {
			return nil, 0, fmt.Errorf("error during scan: %v", err)
		}

		for _, key := range keys {
			for _, callbackKey := range callbackKeys {
				if callbackFunc, ok := callbacks.CallbackMap[callbackKey]; ok {
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

func ScanHandlerBuilder(env, parentNamespace, childNamespace, entity, pattern string, redisClient *redis.Client, callbackKeys ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		envParam := r.URL.Query().Get(env)
		parentNamespaceParam := r.URL.Query().Get(parentNamespace)
		childNamespaceParam := r.URL.Query().Get(childNamespace)
		startingCursorParam := r.URL.Query().Get("cursor")

		pattern := fmt.Sprintf(pattern, entity, envParam, parentNamespaceParam, childNamespaceParam)

		keys, nextCursor, err := RunScan(pattern, parseCursor(startingCursorParam), redisClient, callbackKeys...)
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

func parseCursor(cursorStr string) uint64 {
	cursor, _ := strconv.ParseUint(cursorStr, 10, 64)
	return cursor
}
