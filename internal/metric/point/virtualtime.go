package point

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"time"
)

func NewVirtualTimeMetricPoint(
	virtualTimeName VirtualTimeName,
	objKind ObjectKindType,
	objNamespace string,
	objName string,
	ownerKind ObjectKindType,
	ownerName string,
	userID string,
	evaluationID string,
	scenarioID string,
	iteration int,
	virtualTime time.Time,
	realTime time.Time,
) (MetricPoint, error) {
	if virtualTimeName.String() == "unknown" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: invalid virtualTimeName")
	}
	if objKind == UnknownKind {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: invalid kind")
	}
	if objKind != ScenarioKind && objNamespace == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: namespace is required")
	}
	if objKind != ScenarioKind && objName == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: name is required")
	}
	if ownerKind == UnknownKind {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: invalid ownerKind")
	}
	if ownerKind != NoOwnerKind && ownerName == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: ownerName is required when ownerKind is set")
	}
	if userID == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: userID is required")
	}
	if evaluationID == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: evaluationID is required")
	}
	if scenarioID == "" {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: scenarioID is required")
	}
	if iteration < 0 {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: iteration must be non-negative")
	}
	if virtualTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: virtualTime is required")
	}
	if realTime.IsZero() {
		return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: realTime is required")
	}
	if objKind == PodKind && ownerKind != NoOwnerKind {
		if virtualTimeName == VirtualDeadline {
			return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: pod with owner cannot have VirtualDeadline")
		}
	}
	if objKind == ReplicasetKind {
		if ownerKind != NoOwnerKind {
			return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: replicaset with owner not supported")
		}
		if virtualTimeName == VirtualScheduledAt {
			return nil, fmt.Errorf("metricpoint.NewVirtualTimeMetricPoint: replicaset cannot have VirtualScheduledAt")
		}
	}
	return &virtualTimeMetricPoint{
		virtualTimeName: virtualTimeName,
		objKind:         objKind,
		objNamespace:    objNamespace,
		objName:         objName,
		ownerKind:       ownerKind,
		ownerName:       ownerName,
		userID:          userID,
		evaluationID:    evaluationID,
		scenarioID:      scenarioID,
		iteration:       iteration,
		virtualTime:     virtualTime,
		realTime:        realTime,
	}, nil
}

type virtualTimeMetricPoint struct {
	virtualTimeName VirtualTimeName
	objKind         ObjectKindType
	objNamespace    string
	objName         string
	ownerKind       ObjectKindType
	ownerName       string

	userID       string
	evaluationID string
	scenarioID   string
	iteration    int

	virtualTime time.Time
	realTime    time.Time
}

func (p *virtualTimeMetricPoint) Name() string {
	return "virtualtime"
}

func (p *virtualTimeMetricPoint) ToJSONLine() (string, error) {
	data := jsonlData{
		Metric: metricMetadata{
			Name: metricNameOf(p.Name(), p.virtualTimeName.String(), milisecondsUnit),
			Labels: map[string]string{
				"obj_kind":      p.objKind.String(),
				"obj_namespace": p.objNamespace,
				"obj_name":      p.objName,
				"owner_kind":    p.ownerKind.String(),
				"owner_name":    p.ownerName,
				"user_id":       p.userID,
				"evaluation_id": p.evaluationID,
				"scenario_id":   p.scenarioID,
				"iteration":     strconv.Itoa(p.iteration),
			},
		},
		Values:     []float64{float64(p.virtualTime.UnixMilli())},
		Timestamps: []int64{p.realTime.UnixMilli()},
	}
	line, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(line), nil
}

type VirtualTimeName int

const (
	VirtualCreatedAt VirtualTimeName = iota
	VirtualScheduledAt
	VirtualDeletedAt
	VirtualDeadline
)

func (t VirtualTimeName) String() string {
	switch t {
	case VirtualCreatedAt:
		return "created_at"
	case VirtualScheduledAt:
		return "scheduled_at"
	case VirtualDeletedAt:
		return "deleted_at"
	case VirtualDeadline:
		return "deadline"
	default:
		return "unknown"
	}
}

func (t VirtualTimeName) LogValue() slog.Value {
	return slog.StringValue(t.String())
}

type ObjectKindType int

const (
	NoOwnerKind ObjectKindType = iota
	PodKind
	ReplicasetKind
	ScenarioKind
	UnknownKind
)

func (t ObjectKindType) String() string {
	switch t {
	case NoOwnerKind:
		return "noowner"
	case PodKind:
		return "pod"
	case ReplicasetKind:
		return "replicaset"
	case ScenarioKind:
		return "scenario"
	default:
		return "unknown"
	}
}

func (t ObjectKindType) LogValue() slog.Value {
	return slog.StringValue(t.String())
}

func KindTypeFromString(s string) ObjectKindType {
	switch s {
	case "NoOwner", "Noowner", "noowner":
		return NoOwnerKind
	case "Pod", "pod":
		return PodKind
	case "ReplicaSet", "Replicaset", "replicaset":
		return ReplicasetKind
	case "Scenario", "scenario":
		return ScenarioKind
	default:
		return UnknownKind
	}
}
