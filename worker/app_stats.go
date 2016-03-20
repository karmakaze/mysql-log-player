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
		statsdClient: statsdClient,
	}
}

func (a AppStats) Run() {
	for stat := range a.stats {
		if stat.userId > 0 {
			a.userIds[stat.userId] = struct{}{}
			a.statsdClient.Gauge("number.user_ids", float64(len(a.userIds)))
		}
		if stat.photoId > 0 {
			a.photoIds[stat.photoId] = struct{}{}
			a.statsdClient.Gauge("number.photo_ids", float64(len(a.photoIds)))
		}
	}
}
