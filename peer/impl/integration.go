package impl

func encryptSymmetricKey(symmetricKey []byte, keyID string) ([]byte, error) {
	return symmetricKey, nil
}

func decryptSymmetricKey(encryptedSymmetricKey []byte, keyID string) ([]byte, error) {
	return encryptedSymmetricKey, nil
}

func (s *StreamInfo) getGrade(streamID string) (float64, error) {
	return 0, nil
}
