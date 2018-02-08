package fake

import (
	"sync/atomic"

	"k8s.io/apiserver/pkg/admission/plugin/webhook/config"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/testcerts"
	"k8s.io/client-go/rest"
)

func NewAuthenticationInfoResolver(count *int32) config.AuthenticationInfoResolver {
	return &authenticationInfoResolver{
		restConfig: &rest.Config{
			TLSClientConfig: rest.TLSClientConfig{
				CAData:   testcerts.CACert,
				CertData: testcerts.ClientCert,
				KeyData:  testcerts.ClientKey,
			},
		},
		cachedCount: count,
	}
}

type authenticationInfoResolver struct {
	restConfig  *rest.Config
	cachedCount *int32
}

func (a *authenticationInfoResolver) ClientConfigFor(server string) (*rest.Config, error) {
	atomic.AddInt32(a.cachedCount, 1)
	return a.restConfig, nil
}

func (a *authenticationInfoResolver) ClientConfigForService(serviceName, serviceNamespace string) (*rest.Config, error) {
	atomic.AddInt32(a.cachedCount, 1)
	return a.restConfig, nil
}
