package auth

type AuthInfo struct {
	Password string
	ClientID string
}

func NewAuthInfo(pwd, client_id string) (auth_info AuthInfo) {
	return AuthInfo{Password: pwd, ClientID: client_id}
}
