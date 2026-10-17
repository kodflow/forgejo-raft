// Copyright 2026 The Forgejo Authors. All rights reserved.
// SPDX-License-Identifier: GPL-3.0-or-later

package kitsunium

import (
	"bytes"
	"strings"
	"sync"

	"forgejo.org/modules/log"

	sdklogger "github.com/kitsunium/sdk/pkg/v1/observe/logger"
	sdkmetrics "github.com/kitsunium/sdk/pkg/v1/observe/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// The engine's own observability (ADR 0152 D13): its consensus_* metrics go
// to one meter that Forgejo's /metrics exposes, its logs to Forgejo's log.

// Each node records into its own meter, so its per-peer series and latency
// histograms are its own. The process exposes the meter of the node it
// opened last: one node per process in production.
var (
	currentMu    sync.RWMutex
	currentMeter sdkmetrics.FullMeter
)

// newEngineMeter makes a node's meter and makes it the exposed one.
func newEngineMeter() sdkmetrics.FullMeter {
	m := sdkmetrics.NewMeter()
	currentMu.Lock()
	currentMeter = m
	currentMu.Unlock()
	return m
}

// logWriter forwards each line the SDK's text logger writes to Forgejo's
// log, at Info: the engine logs role, membership and snapshot changes at
// Info and administrative operations and stops at Warn, and the line says
// which.
type logWriter struct{}

func (logWriter) Write(p []byte) (int, error) {
	for line := range strings.SplitSeq(strings.TrimRight(string(p), "\n"), "\n") {
		if line == "" {
			continue
		}
		if strings.Contains(line, "level=WARN") || strings.Contains(line, "level=ERROR") {
			log.Warn("consensus: %s", line)
		} else {
			log.Info("consensus: %s", line)
		}
	}
	return len(p), nil
}

// engineLogger returns the SDK's text logger writing to Forgejo's log, or
// nil, which disables it, when it cannot be built.
func engineLogger() sdklogger.Logger {
	lg, err := sdklogger.NewText(sdklogger.Config{Writer: logWriter{}, MinLevel: sdklogger.LevelInfo})
	if err != nil {
		log.Warn("cluster: the consensus engine's logger could not be built: %v", err)
		return nil
	}
	return lg
}

// engineCollector exposes the meter's series on Forgejo's /metrics. It is
// an unchecked collector: the engine decides its series, and the meter's own
// Prometheus encoding is read back and re-emitted.
type engineCollector struct {
	meter func() sdkmetrics.FullMeter
}

// NewCollector returns the collector of the engine's consensus_* metrics,
// those of the node this process opened.
func NewCollector() prometheus.Collector {
	return engineCollector{meter: func() sdkmetrics.FullMeter {
		currentMu.RLock()
		defer currentMu.RUnlock()
		return currentMeter
	}}
}

func (engineCollector) Describe(chan<- *prometheus.Desc) {}

func (c engineCollector) Collect(ch chan<- prometheus.Metric) {
	m := c.meter()
	if m == nil {
		return
	}
	var buf bytes.Buffer
	if err := sdkmetrics.NewPrometheusExporter("forgejo", &buf).Export(m.Collect()); err != nil {
		log.Warn("cluster: exporting the consensus metrics: %v", err)
		return
	}
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(&buf)
	if err != nil {
		log.Warn("cluster: reading the consensus metrics: %v", err)
		return
	}
	for name, mf := range families {
		for _, m := range mf.GetMetric() {
			var keys, values []string
			for _, l := range m.GetLabel() {
				keys = append(keys, l.GetName())
				values = append(values, l.GetValue())
			}
			desc := prometheus.NewDesc(name, mf.GetHelp(), keys, nil)
			var metric prometheus.Metric
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				metric, err = prometheus.NewConstMetric(desc, prometheus.CounterValue, m.GetCounter().GetValue(), values...)
			case dto.MetricType_GAUGE:
				metric, err = prometheus.NewConstMetric(desc, prometheus.GaugeValue, m.GetGauge().GetValue(), values...)
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				buckets := map[float64]uint64{}
				for _, b := range h.GetBucket() {
					buckets[b.GetUpperBound()] = b.GetCumulativeCount()
				}
				metric, err = prometheus.NewConstHistogram(desc, h.GetSampleCount(), h.GetSampleSum(), buckets, values...)
			default:
				metric, err = prometheus.NewConstMetric(desc, prometheus.UntypedValue, m.GetUntyped().GetValue(), values...)
			}
			if err == nil {
				ch <- metric
			}
		}
	}
}
