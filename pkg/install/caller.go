package install

import (
	"context"
	"errors"
	"fmt"

	authv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// CallerIdentity reports the API server identity the installation writes as,
// by asking the server to review the request. An installation driven by a
// kubeconfig has no service account to guess: only the server knows the name,
// and the install invariant must judge that principal's writes (C-15).
func CallerIdentity(ctx context.Context, client kubernetes.Interface) (string, error) {
	if client == nil {
		return "", errors.New("installation needs a kubernetes client to read its identity")
	}
	review, err := client.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("read the installation identity: %w", err)
	}
	username := review.Status.UserInfo.Username
	if username == "" {
		return "", fmt.Errorf("the API server reported no identity for the installation")
	}
	return username, nil
}
