package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// Google OIDC code 교환 + id_token 검증 (ADR-025, silverone 2026-06-12).
//
// 인터페이스로 분리해 service/테스트가 fake로 대체 가능하게 한다. v1 실구현은
// stdlib HTTP: ① authorization code를 Google token endpoint와 교환해 id_token을
// 받고, ② tokeninfo endpoint로 id_token을 검증(서명/만료는 Google이 서버측 검증)
// 한 뒤 claim을 신뢰한다. JWKS 로컬 RS256 검증은 hardening 후속(외부 호출 1회 제거).

// GoogleAuthenticator — authorization code → 검증된 GoogleClaims.
type GoogleAuthenticator interface {
	Exchange(ctx context.Context, code string) (domain.GoogleClaims, error)
}

const (
	googleTokenURL     = "https://oauth2.googleapis.com/token"
	googleTokenInfoURL = "https://oauth2.googleapis.com/tokeninfo"
)

// httpGoogleAuthenticator — 실 Google 연동.
type httpGoogleAuthenticator struct {
	clientID    string
	secret      string
	redirectURL string
	httpClient  *http.Client
}

// NewGoogleAuthenticator — config 기반 실 authenticator.
func NewGoogleAuthenticator(clientID, secret, redirectURL string) GoogleAuthenticator {
	return &httpGoogleAuthenticator{
		clientID:    clientID,
		secret:      secret,
		redirectURL: redirectURL,
		httpClient:  &http.Client{Timeout: 15 * time.Second},
	}
}

func (g *httpGoogleAuthenticator) Exchange(ctx context.Context, code string) (domain.GoogleClaims, error) {
	idToken, err := g.exchangeCode(ctx, code)
	if err != nil {
		return domain.GoogleClaims{}, err
	}
	return g.verifyIDToken(ctx, idToken)
}

// exchangeCode — authorization code를 token endpoint와 교환해 id_token 회수.
func (g *httpGoogleAuthenticator) exchangeCode(ctx context.Context, code string) (string, error) {
	form := url.Values{}
	form.Set("code", code)
	form.Set("client_id", g.clientID)
	form.Set("client_secret", g.secret)
	form.Set("redirect_uri", g.redirectURL)
	form.Set("grant_type", "authorization_code")

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, googleTokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("google token exchange: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		IDToken string `json:"id_token"`
		Error   string `json:"error"`
		ErrDesc string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", fmt.Errorf("google token exchange decode: %w", err)
	}
	if resp.StatusCode != http.StatusOK || body.IDToken == "" {
		return "", fmt.Errorf("google token exchange failed: %s %s", body.Error, body.ErrDesc)
	}
	return body.IDToken, nil
}

// verifyIDToken — tokeninfo로 검증(Google이 서명/exp 서버측 검증) 후 claim 신뢰.
// aud/iss/email_verified는 우리가 한 번 더 확인한다.
func (g *httpGoogleAuthenticator) verifyIDToken(ctx context.Context, idToken string) (domain.GoogleClaims, error) {
	u := googleTokenInfoURL + "?id_token=" + url.QueryEscape(idToken)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return domain.GoogleClaims{}, err
	}
	resp, err := g.httpClient.Do(req)
	if err != nil {
		return domain.GoogleClaims{}, fmt.Errorf("google tokeninfo: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return domain.GoogleClaims{}, ErrUnauthorized{Message: "google id_token 검증 실패"}
	}
	// tokeninfo는 email_verified 등 bool도 문자열("true")로 줄 수 있어 string으로 받는다.
	var raw struct {
		Aud           string `json:"aud"`
		Iss           string `json:"iss"`
		Sub           string `json:"sub"`
		Email         string `json:"email"`
		EmailVerified string `json:"email_verified"`
		Name          string `json:"name"`
		Picture       string `json:"picture"`
		Hd            string `json:"hd"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return domain.GoogleClaims{}, fmt.Errorf("google tokeninfo decode: %w", err)
	}
	if raw.Aud != g.clientID {
		return domain.GoogleClaims{}, ErrUnauthorized{Message: "google id_token aud 불일치"}
	}
	if raw.Iss != "accounts.google.com" && raw.Iss != "https://accounts.google.com" {
		return domain.GoogleClaims{}, ErrUnauthorized{Message: "google id_token iss 불일치"}
	}
	return domain.GoogleClaims{
		Sub:           raw.Sub,
		Email:         strings.TrimSpace(raw.Email),
		EmailVerified: strings.EqualFold(strings.TrimSpace(raw.EmailVerified), "true"),
		Name:          raw.Name,
		Picture:       raw.Picture,
		HostedDomain:  raw.Hd,
	}, nil
}
