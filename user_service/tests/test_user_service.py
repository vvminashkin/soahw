import pytest
import json
import sys
from datetime import datetime
sys.path.append('../')

from user_service import app, create_jwt_token
from DB.db import get_user_by_login

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

TEST_USER = {
    "login": "test_user",
    "password": "mock_pass",
    "email": "test@gmail.com"
}

def test_register_new_user(client, mocker):
    mocker.patch('DB.db.get_user_by_login', return_value=None)
    mocker.patch('DB.db.create_user', return_value=type('User', (), {
        'id': 1,
        'login': TEST_USER['login'],
        'email': TEST_USER['email']
    }))

    response = client.post('/users/v1/register', json=TEST_USER)
    
    assert response.status_code == 201
    assert json.loads(response.data) == {"message": "User registered successfully"}

def test_register_existing_user(client, mocker):
    response = client.post('/users/v1/register', json=TEST_USER)
    response = client.post('/users/v1/register', json=TEST_USER)
    
    assert response.status_code == 400
    assert "already exists" in json.loads(response.data)['error']

def test_login_success(client, mocker):

    mock_user = type('User', (), {
        'hashed_password': 'pbkdf2:sha256:...',
        'id': 1,
        'login': TEST_USER['login']
    })
    mocker.patch('DB.db.verify_user_credentials', return_value=mock_user)
    
    response = client.post('/users/v1/login', json={
        "login": TEST_USER['login'],
        "password": TEST_USER['password']
    })
    
    assert response.status_code == 200
    assert 'access_token' in json.loads(response.data)

def test_login_invalid_credentials(client, mocker):
    mocker.patch('DB.db.verify_user_credentials', return_value=None)
    
    response = client.post('/users/v1/login', json={
        "login": "wrong_user",
        "password": "wrong_pass"
    })
    
    assert response.status_code == 401
    assert "Invalid credentials" in json.loads(response.data)['error']

def test_get_user_profile(client, mocker):
    mock_user = type('User', (), {
        'id': 1,
        'login': TEST_USER['login'],
        'email': TEST_USER['email'],
        'first_name': 'John',
        'last_name': 'Doe',
        'birth_date': '1990-01-01',
        'phone_number': '+1234567890',
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    })
    
    mocker.patch('DB.db.get_user_by_id', return_value=mock_user)
    
    test_token = create_jwt_token(mock_user)
    
    response = client.get('/users/v1', 
        headers={'Authorization': f'Bearer {test_token}'}
    )
    
    data = json.loads(response.data)
    assert response.status_code == 200
    assert data['login'] == TEST_USER['login']
    assert data['email'] == TEST_USER['email']

def test_update_user_profile(client, mocker):
    update_data = {
        "first_name": "NewName",
        "email": "new@gmail.com"
    }
    
    updated_user = type('User', (), {
        'id': 1,
        **update_data
    })
    
    mocker.patch('DB.db.update_user', return_value=updated_user)
    
    test_token = create_jwt_token(type('User', (), {'id': 1, 'login':'test'}))
    
    response = client.put('/users/v1',
        headers={'Authorization': f'Bearer {test_token}'},
        json=update_data
    )
    
    assert response.status_code == 200
    assert "updated successfully" in json.loads(response.data)['message']

def test_invalid_token(client):
    response = client.get('/users/v1', 
        headers={'Authorization': 'Bearer invalid_token'}
    )
    
    assert response.status_code == 401
    assert "Invalid token" in json.loads(response.data)['error']
