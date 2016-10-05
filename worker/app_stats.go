package worker

import (
	"github.com/500px/go-utils/metrics"
)

type Stat struct {
	userId  int32
    photoId int32
}

type AppStats struct {
	stats        chan Stat
	userIds      map[int32]struct{}
	photoIds     map[int32]struct{}
	statsdClient metrics.StatsdClient
}

func NewAppStats(stats chan Stat, statsdClient metrics.StatsdClient) *AppStats {
	return &AppStats{
		stats:        stats,
		userIds:      map[int32]struct{}{},
		photoIds:     map[int32]struct{}{},
		statsdClient: statsdClient,
	}
}

func (a AppStats) Run() {
	for _ = range a.stats {
	}
}
