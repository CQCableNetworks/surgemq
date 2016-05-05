package auth

type AuthInfo struct {
	password  []byte
	client_id []byte
}

func (this *AuthInfo) Password() string {
	return string(this.password)
}

func (this *AuthInfo) ClientID() string {
	return string(this.client_id)
}

func NewAuthInfo(pwd, client_id []byte) (auth_info *AuthInfo) {
	return &AuthInfo{password: pwd, client_id: client_id}
}
