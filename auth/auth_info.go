package auth

type AuthInfo struct {
	password  []byte
	client_id []byte
	atomic_id uint64
}

func (this *AuthInfo) Password() string {
	return string(this.password)
}

func (this *AuthInfo) ClientID() string {
	return string(this.client_id)
}

func (this *AuthInfo) AtomicID() uint64 {
	return this.atomic_id
}

func NewAuthInfo(pwd, client_id []byte, atomic_id uint64) (auth_info *AuthInfo) {
	return &AuthInfo{password: pwd, client_id: client_id, atomic_id: atomic_id}
}
