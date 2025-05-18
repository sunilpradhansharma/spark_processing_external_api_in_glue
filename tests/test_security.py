import pytest
import jwt
import time
from datetime import datetime, timedelta
from src.processor import SecurityManager

class TestSecurityManager:
    @pytest.fixture
    def security_manager(self):
        return SecurityManager()
        
    def test_payload_encryption(self, security_manager):
        """Test payload encryption and decryption"""
        original_data = {"test": "data", "number": 123}
        encrypted = security_manager.encrypt_payload(original_data)
        decrypted = security_manager.decrypt_payload(encrypted)
        assert decrypted == original_data
        
    def test_jwt_token(self, security_manager):
        """Test JWT token generation and verification"""
        payload = {
            "user_id": "test123",
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = security_manager.generate_token(payload)
        verified = security_manager.verify_token(token)
        assert verified["user_id"] == payload["user_id"]
        
    def test_rate_limiting(self, security_manager):
        """Test IP-based rate limiting"""
        ip = "192.168.1.1"
        limit = 5
        window = 1
        
        # Should allow up to limit requests
        for _ in range(limit):
            assert not security_manager.is_rate_limited(ip, limit, window)
            
        # Should rate limit after limit is exceeded
        assert security_manager.is_rate_limited(ip, limit, window)
        
    def test_rate_limit_window(self, security_manager):
        """Test rate limit window expiration"""
        ip = "192.168.1.2"
        limit = 2
        window = 1
        
        # Make initial requests
        assert not security_manager.is_rate_limited(ip, limit, window)
        assert not security_manager.is_rate_limited(ip, limit, window)
        assert security_manager.is_rate_limited(ip, limit, window)
        
        # Wait for window to expire
        time.sleep(window + 0.1)
        assert not security_manager.is_rate_limited(ip, limit, window)
        
    def test_invalid_token(self, security_manager):
        """Test invalid token handling"""
        with pytest.raises(jwt.InvalidTokenError):
            security_manager.verify_token("invalid.token.here")
            
    def test_expired_token(self, security_manager):
        """Test expired token handling"""
        payload = {
            "user_id": "test123",
            "exp": datetime.utcnow() - timedelta(hours=1)
        }
        token = security_manager.generate_token(payload)
        with pytest.raises(jwt.ExpiredSignatureError):
            security_manager.verify_token(token) 