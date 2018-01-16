package generic

import "k8s.io/api/admissionregistration/v1beta1"

// Source can list dynamic webhook plugins.
type Source interface {
	Webhooks() []v1beta1.Webhook
}
